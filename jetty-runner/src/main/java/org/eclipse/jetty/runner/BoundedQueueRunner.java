package org.eclipse.jetty.runner;

import org.eclipse.jetty.io.ConnectionStatistics;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.*;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlet.StatisticsServlet;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.RolloverFileOutputStream;
import org.eclipse.jetty.util.StringUtil;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.webapp.WebInfConfiguration;
import org.eclipse.jetty.xml.XmlConfiguration;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;

/**
 * Created by karthik on 09/03/17.
 */
public class BoundedQueueRunner extends Runner {
    private static final Logger LOG = Log.getLogger(BoundedQueueRunner.class);

    /**
     * Configure a jetty instance and deploy the webapps presented as args
     *
     * @param args the command line arguments
     * @throws Exception if unable to configure
     */
    public void configure(String[] args) throws Exception {
        // handle classpath bits first so we can initialize the log mechanism.
        for (int i = 0; i < args.length; i++) {
            if ("--lib".equals(args[i])) {
                try (Resource lib = Resource.newResource(args[++i])) {
                    if (!lib.exists() || !lib.isDirectory())
                        usage("No such lib directory " + lib);
                    _classpath.addJars(lib);
                }
            } else if ("--jar".equals(args[i])) {
                try (Resource jar = Resource.newResource(args[++i])) {
                    if (!jar.exists() || jar.isDirectory())
                        usage("No such jar " + jar);
                    _classpath.addPath(jar);
                }
            } else if ("--classes".equals(args[i])) {
                try (Resource classes = Resource.newResource(args[++i])) {
                    if (!classes.exists() || !classes.isDirectory())
                        usage("No such classes directory " + classes);
                    _classpath.addPath(classes);
                }
            } else if (args[i].startsWith("--"))
                i++;
        }

        initClassLoader();

        LOG.info("Runner");
        LOG.debug("Runner classpath {}", _classpath);
        LOG.info("Warning: This version of Jetty-runner is customized for VIS (Voucher Inventory Service) with a BlockingArrayQueue with maxCapacity");

        String contextPath = __defaultContextPath;
        boolean contextPathSet = false;
        int port = __defaultPort;
        String host = null;
        int stopPort = 0;
        String stopKey = null;

        boolean runnerServerInitialized = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                    port = Integer.parseInt(args[++i]);
                    break;
                case "--host":
                    host = args[++i];
                    break;
                case "--stop-port":
                    stopPort = Integer.parseInt(args[++i]);
                    break;
                case "--stop-key":
                    stopKey = args[++i];
                    break;
                case "--log":
                    _logFile = args[++i];
                    break;
                case "--out":
                    String outFile = args[++i];
                    PrintStream out = new PrintStream(new RolloverFileOutputStream(outFile, true, -1));
                    LOG.info("Redirecting stderr/stdout to " + outFile);
                    System.setErr(out);
                    System.setOut(out);
                    break;
                case "--path":
                    contextPath = args[++i];
                    contextPathSet = true;
                    break;
                case "--config":
                    if (_configFiles == null)
                        _configFiles = new ArrayList<>();
                    _configFiles.add(args[++i]);
                    break;
                case "--lib":
                    ++i;//skip

                    break;
                case "--jar":
                    ++i; //skip

                    break;
                case "--classes":
                    ++i;//skip

                    break;
                case "--stats":
                    _enableStats = true;
                    _statsPropFile = args[++i];
                    _statsPropFile = ("unsecure".equalsIgnoreCase(_statsPropFile) ? null : _statsPropFile);
                    break;
                default:
// process contexts

                    if (!runnerServerInitialized) // log handlers not registered, server maybe not created, etc
                    {
                        if (_server == null) // server not initialized yet
                        {
                            // build the server
                            LOG.info("[VIS] Initiating a new server instance");

                            _server = new Server(createInitialThreadPool());
                        }

                        //apply jetty config files if there are any
                        if (_configFiles != null) {
                            for (String cfg : _configFiles) {
                                try (Resource resource = Resource.newResource(cfg)) {
                                    XmlConfiguration xmlConfiguration = new XmlConfiguration(resource.getURL());
                                    xmlConfiguration.configure(_server);
                                }
                            }
                        }

                        //check that everything got configured, and if not, make the handlers
                        HandlerCollection handlers = (HandlerCollection) _server.getChildHandlerByClass(HandlerCollection.class);
                        if (handlers == null) {
                            handlers = new HandlerCollection();
                            _server.setHandler(handlers);
                        }

                        //check if contexts already configured
                        _contexts = (ContextHandlerCollection) handlers.getChildHandlerByClass(ContextHandlerCollection.class);
                        if (_contexts == null) {
                            _contexts = new ContextHandlerCollection();
                            prependHandler(_contexts, handlers);
                        }


                        if (_enableStats) {
                            //if no stats handler already configured
                            if (handlers.getChildHandlerByClass(StatisticsHandler.class) == null) {
                                StatisticsHandler statsHandler = new StatisticsHandler();


                                Handler oldHandler = _server.getHandler();
                                statsHandler.setHandler(oldHandler);
                                _server.setHandler(statsHandler);


                                ServletContextHandler statsContext = new ServletContextHandler(_contexts, "/stats");
                                statsContext.addServlet(new ServletHolder(new StatisticsServlet()), "/");
                                statsContext.setSessionHandler(new SessionHandler());
                                if (_statsPropFile != null) {
                                    HashLoginService loginService = new HashLoginService("StatsRealm", _statsPropFile);
                                    Constraint constraint = new Constraint();
                                    constraint.setName("Admin Only");
                                    constraint.setRoles(new String[]{"admin"});
                                    constraint.setAuthenticate(true);

                                    ConstraintMapping cm = new ConstraintMapping();
                                    cm.setConstraint(constraint);
                                    cm.setPathSpec("/*");

                                    ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();
                                    securityHandler.setLoginService(loginService);
                                    securityHandler.setConstraintMappings(Collections.singletonList(cm));
                                    securityHandler.setAuthenticator(new BasicAuthenticator());
                                    statsContext.setSecurityHandler(securityHandler);
                                }
                            }
                        }

                        //ensure a DefaultHandler is present
                        if (handlers.getChildHandlerByClass(DefaultHandler.class) == null) {
                            handlers.addHandler(new DefaultHandler());
                        }

                        //ensure a log handler is present
                        _logHandler = (RequestLogHandler) handlers.getChildHandlerByClass(RequestLogHandler.class);
                        if (_logHandler == null) {
                            _logHandler = new RequestLogHandler();
                            handlers.addHandler(_logHandler);
                        }


                        //check a connector is configured to listen on
                        Connector[] connectors = _server.getConnectors();
                        if (connectors == null || connectors.length == 0) {
                            ServerConnector connector = new ServerConnector(_server);
                            connector.setPort(port);
                            if (host != null)
                                connector.setHost(host);
                            _server.addConnector(connector);
                            if (_enableStats)
                                connector.addBean(new ConnectionStatistics());
                        } else {
                            if (_enableStats) {
                                for (Connector connector : connectors) {
                                    ((AbstractConnector) connector).addBean(new ConnectionStatistics());
                                }
                            }
                        }

                        runnerServerInitialized = true;
                    }

                    // Create a context
                    try (Resource ctx = Resource.newResource(args[i])) {
                        if (!ctx.exists())
                            usage("Context '" + ctx + "' does not exist");

                        if (contextPathSet && !(contextPath.startsWith("/")))
                            contextPath = "/" + contextPath;

                        // Configure the context
                        if (!ctx.isDirectory() && ctx.toString().toLowerCase(Locale.ENGLISH).endsWith(".xml")) {
                            // It is a context config file
                            XmlConfiguration xmlConfiguration = new XmlConfiguration(ctx.getURL());
                            xmlConfiguration.getIdMap().put("Server", _server);
                            ContextHandler handler = (ContextHandler) xmlConfiguration.configure();
                            if (contextPathSet)
                                handler.setContextPath(contextPath);
                            _contexts.addHandler(handler);
                            String containerIncludeJarPattern = (String) handler.getAttribute(WebInfConfiguration.CONTAINER_JAR_PATTERN);
                            if (containerIncludeJarPattern == null)
                                containerIncludeJarPattern = __containerIncludeJarPattern;
                            else {
                                if (!containerIncludeJarPattern.contains(__containerIncludeJarPattern)) {
                                    containerIncludeJarPattern = containerIncludeJarPattern + (StringUtil.isBlank(containerIncludeJarPattern) ? "" : "|") + __containerIncludeJarPattern;
                                }
                            }

                            handler.setAttribute(WebInfConfiguration.CONTAINER_JAR_PATTERN, containerIncludeJarPattern);

                            //check the configurations, if not explicitly set up, then configure all of them
                            if (handler instanceof WebAppContext) {
                                WebAppContext wac = (WebAppContext) handler;
                                if (wac.getConfigurationClasses() == null || wac.getConfigurationClasses().length == 0)
                                    wac.setConfigurationClasses(__plusConfigurationClasses);
                            }
                        } else {
                            // assume it is a WAR file
                            WebAppContext webapp = new WebAppContext(_contexts, ctx.toString(), contextPath);
                            webapp.setConfigurationClasses(__plusConfigurationClasses);
                            webapp.setAttribute(WebInfConfiguration.CONTAINER_JAR_PATTERN,
                                    __containerIncludeJarPattern);
                        }
                    }
                    //reset
                    contextPathSet = false;
                    contextPath = __defaultContextPath;
                    break;
            }
        }

        if (_server == null)
            usage("No Contexts defined");
        _server.setStopAtShutdown(true);

        switch ((stopPort > 0 ? 1 : 0) + (stopKey != null ? 2 : 0)) {
            case 1:
                usage("Must specify --stop-key when --stop-port is specified");
                break;

            case 2:
                usage("Must specify --stop-port when --stop-key is specified");
                break;

            case 3:
                ShutdownMonitor monitor = ShutdownMonitor.getInstance();
                monitor.setPort(stopPort);
                monitor.setKey(stopKey);
                monitor.setExitVm(true);
                break;
        }

        if (_logFile != null) {
            NCSARequestLog requestLog = new NCSARequestLog(_logFile);
            requestLog.setExtended(false);
            _logHandler.setRequestLog(requestLog);
        }
    }

    private static ThreadPool createInitialThreadPool() {
        final int DEFAULT_MIN_THREADS = 8;
        final int DEFAULT_ACCEPTOR_COUNT = 8;
        final int DEFAULT_MAX_THREADS = 200;
        final int DEFAULT_IDLE_TIMEOUT = 60000;
        final int DEFAULT_MAX_CAPACITY = Integer.MAX_VALUE;

        String jettyQueueSizeEnv = System.getenv("JETTY_QUEUE_MAX_SIZE");

        int queueSize = StringUtil.isBlank(jettyQueueSizeEnv) ? DEFAULT_MAX_CAPACITY : Integer.parseInt(jettyQueueSizeEnv);
        int capacity = Math.max(DEFAULT_MIN_THREADS, DEFAULT_ACCEPTOR_COUNT);

        BlockingQueue queue = new BlockingArrayQueue<>(capacity, capacity, queueSize);
        ThreadPool threadPool = new QueuedThreadPool(DEFAULT_MAX_THREADS, DEFAULT_MIN_THREADS, DEFAULT_IDLE_TIMEOUT, queue);

        LOG.info("Set threadpool's queue maxCapactiy to " + queueSize);

        return threadPool;
    }

    public static void main(String[] args) {
        Runner runner = new BoundedQueueRunner();

        try {
            if (args.length > 0 && args[0].equalsIgnoreCase("--help")) {
                runner.usage(null);
            } else if (args.length > 0 && args[0].equalsIgnoreCase("--version")) {
                runner.version();
            } else {
                runner.configure(args);
                runner.run();
            }
        } catch (Exception e) {
            e.printStackTrace();
            runner.usage(null);
        }
    }
}
