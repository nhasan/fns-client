package us.dot.faa.swim.fns;

import java.util.Hashtable;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;

import us.dot.faa.swim.fns.fil.FilClientConfig;
import us.dot.faa.swim.fns.notamdb.NotamDbConfig;
import us.dot.faa.swim.jms.JmsClientConfig;

public class FnsClientConfig {

    protected FilClientConfig filClientConfig = new FilClientConfig();
    protected JmsClientConfig jmsClientConfig = new JmsClientConfig();
    protected NotamDbConfig notamDbConfig = new NotamDbConfig();

    protected String jmsConnectionFactoryName = "";
    protected String jmsDestination = "";
    protected int jmsProcessingThreads = 2;

    protected int missedMessageTrackerScheduleRate = 10; // seconds
    protected int missedMessageTriggerTime = 5; // minutes
    protected int staleMessageTriggerTime = 10; // minutes

    protected boolean restApiIsEnabled = true;
    protected int restApiPort = 8080;

    public FnsClientConfig() {
    }

    public FnsClientConfig(Config typeSafeConfig) {
        // set fil config
        this.filClientConfig.setFilSftpHost(typeSafeConfig.getString("fil.sftp.host"));
        this.filClientConfig.setFilSftpUsername(typeSafeConfig.getString("fil.sftp.username"));
        this.filClientConfig.setFilSftpPort(typeSafeConfig.getInt("fil.sftp.port"));
        this.filClientConfig.setFilSftpCertFilePath(typeSafeConfig.getString("fil.sftp.certFilePath"));

        // set jms config
        this.jmsClientConfig.setInitialContextFactory(typeSafeConfig.getString("jms.initialContextFactory"));
        this.jmsClientConfig.setProviderUrl(typeSafeConfig.getString("jms.providerUrl"));
        this.jmsClientConfig.setUsername(typeSafeConfig.getString("jms.username"));
        this.jmsClientConfig.setPassword(typeSafeConfig.getString("jms.password"));
        this.jmsClientConfig.setSolaceMessageVpn(typeSafeConfig.getString("jms.solace.messageVpn"));
        this.jmsClientConfig.setSolaceSslTrustStore(typeSafeConfig.getString("jms.solace.sslTrustStore"));
        this.jmsClientConfig.setSolaceJndiConnectionRetries(typeSafeConfig.getInt("jms.solace.jndiConnectionRetries"));
        this.jmsConnectionFactoryName = typeSafeConfig.getString("jms.connectionFactory");
        this.jmsDestination = typeSafeConfig.getString("jms.destination");

        if (typeSafeConfig.hasPath("jms.additionalJndiProperties")) {
            Hashtable<String, Object> jndiProperties = new Hashtable<>();
            for (final Object jndiPropsObject : typeSafeConfig.getList("jms.jndiProperties")) {
                if (jndiPropsObject instanceof ConfigObject) {
                    final ConfigObject jndiProps = (ConfigObject) jndiPropsObject;
                    jndiProperties.put(jndiProps.get("0").render().toString().replace("\"", ""),
                            jndiProps.get("1").render().toString().replace("\"", ""));
                } else {
                    final ConfigList jndiProps = (ConfigList) jndiPropsObject;
                    jndiProperties.put(jndiProps.get(0).render().toString().replace("\"", ""),
                            jndiProps.get(1).render().toString().replace("\"", ""));
                }
            }

            this.jmsClientConfig.setJndiProperties(jndiProperties);
        }

        // set NotamDb Config
        if(typeSafeConfig.hasPath("notamDb"))
        {
            this.notamDbConfig.setDriver(typeSafeConfig.getString("notamDb.driver"));
            this.notamDbConfig.setConnectionUrl(typeSafeConfig.getString("notamDb.connectionUrl"));
            this.notamDbConfig.setUsername(typeSafeConfig.getString("notamDb.username"));
            this.notamDbConfig.setPassword(typeSafeConfig.getString("notamDb.password"));
            this.notamDbConfig.setSchema(typeSafeConfig.getString("notamDb.schema"));
            this.notamDbConfig.setTable(typeSafeConfig.getString("notamDb.table"));
        }
        
        // set Missed Message Tracker config
        if(typeSafeConfig.hasPath("messageTracker"))
        {
            this.missedMessageTrackerScheduleRate = typeSafeConfig.getInt("messageTracker.scheduleRate");
            this.missedMessageTriggerTime = typeSafeConfig.getInt("messageTracker.missedMessageTriggerTime");
            this.staleMessageTriggerTime = typeSafeConfig.getInt("messageTracker.staleMessageTriggerTime");
        }

        // set Rest Api
        if(typeSafeConfig.hasPath("restapi"))
        {
            this.restApiIsEnabled = typeSafeConfig.getBoolean("restapi.enabled");
            this.restApiPort = typeSafeConfig.getInt("restapi.port");
        }
    }

    // getters
    public FilClientConfig getFilClientConfig() {
        return this.filClientConfig;
    }

    public JmsClientConfig getJmsClientConfig() {
        return this.jmsClientConfig;
    }

    public NotamDbConfig getNotamDbConfig() {
        return this.notamDbConfig;
    }

    public String getJmsConnectionFactoryName() {
        return this.jmsConnectionFactoryName;
    }

    public String getJmsDestination() {
        return this.jmsDestination;
    }

    public int getJmsProcessingThreads() {
        return this.jmsProcessingThreads;
    }

    public int getMissedMessageTrackerScheduleRate() {
        return this.missedMessageTrackerScheduleRate;
    }

    public int getMissedMessageTriggerTime() {
        return this.missedMessageTriggerTime;
    }

    public int getStaleMessageTriggerTime() {
        return this.staleMessageTriggerTime;
    }

    public boolean getRestApiIsEnabled() {
        return this.restApiIsEnabled;
    }

    public int getRestApiPort() {
        return this.restApiPort;
    }

    // setters
    public FnsClientConfig setFilClientConfig(FilClientConfig filClientConfig) {
        this.filClientConfig = filClientConfig;
        return this;
    }

    public FnsClientConfig setJmsClientConfig(JmsClientConfig jmsClientConfig) {
        this.jmsClientConfig = jmsClientConfig;
        return this;
    }

    public FnsClientConfig setNotamDbConfig(NotamDbConfig notamDbConfig) {
        this.notamDbConfig = notamDbConfig;
        return this;
    }

    public FnsClientConfig setJmsConnectionFactoryName(String jmsConnectionFactoryName) {
        this.jmsConnectionFactoryName = jmsConnectionFactoryName;
        return this;
    }

    public FnsClientConfig setJmsDestination(String jmsDestination) {
        this.jmsDestination = jmsDestination;
        return this;
    }

    public FnsClientConfig setJmsProcessingThreads(int jmsProcessingThreads) {
        this.jmsProcessingThreads = jmsProcessingThreads;
        return this;
    }

    public FnsClientConfig setMissedMessageTrackerScheduleRate(int missedMessageTrackerScheduleRate) {
        this.missedMessageTrackerScheduleRate = missedMessageTrackerScheduleRate;
        return this;
    }

    public FnsClientConfig setMissedMessageTriggerTime(int missedMessageTriggerTime) {
        this.missedMessageTriggerTime = missedMessageTriggerTime;
        return this;
    }

    public FnsClientConfig setStaleMessageTriggerTime(int staleMessageTriggerTime) {
        this.staleMessageTriggerTime = staleMessageTriggerTime;
        return this;
    }

    public FnsClientConfig setRestApiIsEnabled(boolean restApiIsEnabled) {
        this.restApiIsEnabled = restApiIsEnabled;
        return this;
    }

    public FnsClientConfig setRestApiPort(int restApiPort) {
        this.restApiPort = restApiPort;
        return this;
    }
}
