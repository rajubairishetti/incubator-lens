/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.metrics;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.lens.api.query.QueryStatus.Status;
import org.apache.lens.server.LensService;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.events.AsyncEventListener;
import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.api.metrics.DisabledMethodMetricsContext;
import org.apache.lens.server.api.metrics.LensMetricsRegistry;
import org.apache.lens.server.api.metrics.MethodMetricsContext;
import org.apache.lens.server.api.metrics.MethodMetricsFactory;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.api.query.StatusChange;
import org.apache.lens.server.api.session.SessionEvent;
import org.apache.lens.server.api.session.SessionService;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.AbstractService;
import org.apache.log4j.Logger;

import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.model.ResourceMethod;

import com.codahale.metrics.*;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;
import lombok.Getter;

/**
 * The Class MetricsServiceImpl.
 */
public class MetricsServiceImpl extends AbstractService implements MetricsService {

  /** The Constant LOG. */
  public static final Logger LOG = Logger.getLogger(MetricsService.class);

  /** The query status listener. */
  private AsyncEventListener<StatusChange> queryStatusListener;

  private AsyncEventListener sessionEventListener;

  /** The metric registry. */
  @Getter
  private MetricRegistry metricRegistry;

  /** The reporters. */
  @Getter
  private List<ScheduledReporter> reporters;

  /** The health check. */
  @Getter
  private HealthCheckRegistry healthCheck;

  private Counter totalOpenedSessions;

  private Counter totalClosedSessions;

  private Counter totalExpiredSessions;

  private Counter totalRestoredSessions;

  /** The total accepted queries. */
  private Counter totalAcceptedQueries;

  /** The total successful queries. */
  private Counter totalSuccessfulQueries;

  /** The total finished queries. */
  private Counter totalFinishedQueries;

  /** The total failed queries. */
  private Counter totalFailedQueries;

  /** The total cancelled queries. */
  private Counter totalCancelledQueries;

  private Gauge<Integer> opendSessions;

  private Gauge<Integer> closedSessions;

  private Gauge<Integer> restoredSessions;

  private Gauge<Integer> expiredSessions;

  /** The queued queries. */
  private Gauge<Long> queuedQueries;

  /** The running queries. */
  private Gauge<Long> runningQueries;

  /** The finished queries. */
  private Gauge<Long> finishedQueries;

  /** All method meters. Factory for creation + caching */
  @Getter
  private MethodMetricsFactory methodMetricsFactory;

  @Getter
  private boolean enableResourceMethodMetering;

  public void setEnableResourceMethodMetering(boolean enableResourceMethodMetering) {
    LOG.info("setEnableResourceMethodMetering: " + enableResourceMethodMetering);
    this.enableResourceMethodMetering = enableResourceMethodMetering;
    if (!enableResourceMethodMetering) {
      methodMetricsFactory.clear();
    }
  }

  /**
   * The listener interface for receiving asyncQueryStatus events. The class that is interested in processing a
   * asyncQueryStatus event implements this interface, and the object created with that class is registered with a
   * component using the component's <code>addAsyncQueryStatusListener<code> method. When the asyncQueryStatus event
   * occurs, that object's appropriate method is invoked.
   */
  public class AsyncQueryStatusListener extends AsyncEventListener<StatusChange> {

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lens.server.api.events.AsyncEventListener#process(org.apache.lens.server.api.events.LensEvent)
     */
    @Override
    public void process(StatusChange event) {
      processCurrentStatus(event.getCurrentValue());
    }

    /**
     * Process current status.
     *
     * @param currentValue the current value
     */
    protected void processCurrentStatus(Status currentValue) {
      switch (currentValue) {
      case QUEUED:
        totalAcceptedQueries.inc();
        break;
      case CANCELED:
        totalCancelledQueries.inc();
        totalFinishedQueries.inc();
        break;
      case FAILED:
        totalFailedQueries.inc();
        totalFinishedQueries.inc();
        break;
      case SUCCESSFUL:
        totalSuccessfulQueries.inc();
        totalFinishedQueries.inc();
        break;
      default:
        break;
      }
    }
  }

  /**
   * The listener interface for receiving asyncQueryStatus events. The class that is interested in processing a
   * asyncQueryStatus event implements this interface, and the object created with that class is registered with a
   * component using the component's <code>addAsyncQueryStatusListener<code> method. When the asyncQueryStatus event
   * occurs, that object's appropriate method is invoked.
   */
  public class AsyncSessionEventListener extends AsyncEventListener<SessionEvent> {

    /*
     * (non-Javadoc)
     *
     * @see org.apache.lens.server.api.events.AsyncEventListener#process(org.apache.lens.server.api.events.LensEvent)
     */
    @Override
    public void process(SessionEvent event) {
      System.out.println("BBBBBBBBBBBBBBBBBBBBBBBBBB evnttttttttttt " + event + "  CC " + event.getSessionStatus());
      processCurrentStatus(event.getSessionStatus());
    }

    /**
     * Process current status.
     *
     * @param currentValue the current value
     */
    protected void processCurrentStatus(org.apache.lens.server.api.session.SessionEvent.Status sessionStatus) {
      switch (sessionStatus) {
      case OPENED:
        totalOpenedSessions.inc();
        break;
      case CLOSED:
        totalClosedSessions.inc();
        totalOpenedSessions.dec();
        break;
      default:
        break;
      }
    }
  }

  /**
   * Instantiates a new metrics service impl.
   *
   * @param name the name
   */
  public MetricsServiceImpl(String name) {
    super(NAME);
  }

  private QueryExecutionService getQuerySvc() {
    return (QueryExecutionService) LensServices.get().getService(QueryExecutionService.NAME);
  }

  private SessionService getSessionSvc() {
    return (SessionService) LensServices.get().getService(SessionService.NAME);
  }
  /** The time between polls. */
  private static int timeBetweenPolls = 10;

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hive.service.AbstractService#init(org.apache.hadoop.hive.conf.HiveConf)
   */
  @Override
  public synchronized void init(HiveConf hiveConf) {
    queryStatusListener = new AsyncQueryStatusListener();
    sessionEventListener = new AsyncSessionEventListener();
    LensEventService eventService = (LensEventService) LensServices.get().getService(LensEventService.NAME);
    eventService.addListenerForType(queryStatusListener, StatusChange.class);
    eventService.addListenerForType(sessionEventListener, SessionEvent.class);
    metricRegistry = LensMetricsRegistry.getStaticRegistry();
    methodMetricsFactory = new MethodMetricsFactory(metricRegistry);
    setEnableResourceMethodMetering(hiveConf.getBoolean(LensConfConstants.ENABLE_RESOURCE_METHOD_METERING, false));
    healthCheck = new HealthCheckRegistry();
    initCounters();
    timeBetweenPolls = hiveConf.getInt(LensConfConstants.REPORTING_PERIOD, 10);

    reporters = new ArrayList<ScheduledReporter>();
    if (hiveConf.getBoolean(LensConfConstants.ENABLE_CONSOLE_METRICS, false)) {
      // Start console reporter
      ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS).build();
      reporters.add(reporter);
    }
    if (hiveConf.getBoolean(LensConfConstants.ENABLE_CSV_METRICS, false)) {
      File f = new File(hiveConf.get(LensConfConstants.CSV_METRICS_DIRECTORY_PATH, "metrics/"));
      f.mkdirs();
      CsvReporter reporter = CsvReporter.forRegistry(metricRegistry)
        .formatFor(Locale.US)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build(f);
      reporters.add(reporter);
    }
    if (hiveConf.getBoolean(LensConfConstants.ENABLE_GANGLIA_METRICS, false)) {
      GMetric ganglia;
      try {
        ganglia = new GMetric(hiveConf.get(LensConfConstants.GANGLIA_SERVERNAME), hiveConf.getInt(
          LensConfConstants.GANGLIA_PORT, LensConfConstants.DEFAULT_GANGLIA_PORT), UDPAddressingMode.MULTICAST, 1);
        GangliaReporter greporter = GangliaReporter.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS).build(ganglia);

        reporters.add(greporter);
      } catch (IOException e) {
        LOG.error("Could not start ganglia reporter", e);
      }
    }
    if (hiveConf.getBoolean(LensConfConstants.ENABLE_GRAPHITE_METRICS, false)) {
      final GraphiteReporter reporter;
      try {
        Graphite graphite = new Graphite(new InetSocketAddress(hiveConf.get(LensConfConstants.GRAPHITE_SERVERNAME),
          hiveConf.getInt(LensConfConstants.GRAPHITE_PORT, LensConfConstants.DEFAULT_GRAPHITE_PORT)));
        reporter = GraphiteReporter.forRegistry(metricRegistry)
          .prefixedWith(InetAddress.getLocalHost().getHostName())
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .filter(MetricFilter.ALL)
          .build(graphite);
        reporters.add(reporter);
      } catch (UnknownHostException e) {
        LOG.error("Couldn't get localhost. So couldn't setup graphite reporting");
      }
    }
    LOG.info("Started metrics service");
    super.init(hiveConf);
  }

  /**
   * Inits the counters.
   */
  protected void initCounters() {
    opendSessions = metricRegistry.register(MetricRegistry.name(SessionService.class, "openedSessions"),
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return LensService.SESSION_MAP.size();
        }
      });

    queuedQueries = metricRegistry.register(MetricRegistry.name(QueryExecutionService.class, QUEUED_QUERIES),
      new Gauge<Long>() {
        @Override
        public Long getValue() {
          return getQuerySvc().getQueuedQueriesCount();
        }
      });

    runningQueries = metricRegistry.register(MetricRegistry.name(QueryExecutionService.class, RUNNING_QUERIES),
      new Gauge<Long>() {
        @Override
        public Long getValue() {
          return getQuerySvc().getRunningQueriesCount();
        }
      });

    finishedQueries = metricRegistry.register(MetricRegistry.name(QueryExecutionService.class, FINISHED_QUERIES),
      new Gauge<Long>() {
        @Override
        public Long getValue() {
          return getQuerySvc().getFinishedQueriesCount();
        }
      });

    totalAcceptedQueries = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, "total-"
      + ACCEPTED_QUERIES));

    totalSuccessfulQueries = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class,
      "total-success-queries"));

    totalFinishedQueries = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, "total-"
      + FINISHED_QUERIES));

    totalFailedQueries = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, "total-"
      + FAILED_QUERIES));

    totalCancelledQueries = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, "total-"
        + CANCELLED_QUERIES));

    totalOpenedSessions = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, "total-"
        + "openedSessions"));

    totalClosedSessions = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, "total-"
        + "closedSessions"));

    metricRegistry.register("gc", new GarbageCollectorMetricSet());
    metricRegistry.register("memory", new MemoryUsageGaugeSet());
    metricRegistry.register("threads", new ThreadStatesGaugeSet());
    metricRegistry.register("jvm", new JvmAttributeGaugeSet());
  }

  /**
   * Called by LensRequestListener in start event. marks the method meter, starts a timer, returns timer context
   * returned by the start timer call.
   *
   * @param method           the resource method
   * @param containerRequest container request
   * @return
   * @see org.apache.lens.server.api.metrics.MetricsService#getMethodMetricsContext(
   *org.glassfish.jersey.server.model.ResourceMethod, org.glassfish.jersey.server.ContainerRequest)
   */
  @Override
  public MethodMetricsContext getMethodMetricsContext(ResourceMethod method, ContainerRequest containerRequest) {
    // if method is null then it means no matching resource method was found.
    return enableResourceMethodMetering ? methodMetricsFactory.get(method, containerRequest).newContext()
      : DisabledMethodMetricsContext.getInstance();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hive.service.AbstractService#start()
   */
  @Override
  public synchronized void start() {
    for (ScheduledReporter reporter : reporters) {
      reporter.start(timeBetweenPolls, TimeUnit.SECONDS);
    }
    super.start();

  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hive.service.AbstractService#stop()
   */
  @Override
  public synchronized void stop() {
    // unregister
    LensEventService eventService = (LensEventService) LensServices.get().getService(LensEventService.NAME);
    if (eventService != null) {
      eventService.removeListener(queryStatusListener);
    }

    if (queryStatusListener != null) {
      queryStatusListener.stop();
    }

    if (reporters != null) {
      for (ScheduledReporter reporter : reporters) {
        reporter.stop();
      }
    }

    LOG.info("Stopped metrics service");
    super.stop();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.metrics.MetricsService#incrCounter(java.lang.String)
   */
  @Override
  public void incrCounter(String counter) {
    incrCounter(MetricsService.class, counter);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.metrics.MetricsService#decrCounter(java.lang.String)
   */
  @Override
  public void decrCounter(String counter) {
    decrCounter(MetricsService.class, counter);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.metrics.MetricsService#incrCounter(java.lang.Class, java.lang.String)
   */
  @Override
  public void incrCounter(Class<?> cls, String counter) {
    metricRegistry.counter(MetricRegistry.name(cls, counter)).inc();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.metrics.MetricsService#decrCounter(java.lang.Class, java.lang.String)
   */
  @Override
  public void decrCounter(Class<?> cls, String counter) {
    metricRegistry.counter(MetricRegistry.name(cls, counter)).dec();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.metrics.MetricsService#getCounter(java.lang.String)
   */
  @Override
  public long getCounter(String counter) {
    return getCounter(MetricsService.class, counter);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.metrics.MetricsService#getCounter(java.lang.Class, java.lang.String)
   */
  @Override
  public long getCounter(Class<?> cls, String counter) {
    return metricRegistry.counter(MetricRegistry.name(cls, counter)).getCount();
  }

  @Override
  public long getQueuedQueries() {
    return queuedQueries.getValue();
  }

  @Override
  public long getRunningQueries() {
    return runningQueries.getValue();
  }

  @Override
  public long getFinishedQueries() {
    return finishedQueries.getValue();
  }

  @Override
  public long getTotalAcceptedQueries() {
    return totalAcceptedQueries.getCount();
  }

  @Override
  public long getTotalFinishedQueries() {
    return totalFinishedQueries.getCount();
  }

  @Override
  public long getTotalCancelledQueries() {
    return totalCancelledQueries.getCount();
  }

  @Override
  public long getTotalFailedQueries() {
    return totalFailedQueries.getCount();
  }

  @Override
  public int getOpenedSessions() {
    return opendSessions.getValue();
  }

  @Override
  public long getTotalOpenedSessions() {
    return totalOpenedSessions.getCount();
  }

  @Override
  public long getTotalClosedSessions() {
    return totalClosedSessions.getCount();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.metrics.MetricsService#publishReport()
   */
  @Override
  public void publishReport() {
    if (reporters != null) {
      for (ScheduledReporter reporter : reporters) {
        reporter.report();
      }
    }
  }

  @Override
  public long getTotalSuccessfulQueries() {
    return totalSuccessfulQueries.getCount();
  }

}
