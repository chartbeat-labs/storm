package storm.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

public class TransactionalKafkaSpout extends KafkaSpout {

	public TransactionalKafkaSpout(SpoutConfig spoutConf) {
		super(spoutConf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;

		Map stateConf = new HashMap(conf);
		List<String> zkServers = _spoutConfig.zkServers;
		if (zkServers == null) {
			zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
		}
		Integer zkPort = _spoutConfig.zkPort;
		if (zkPort == null) {
			zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
		}
		stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, conf.get(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS));
		stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, ((Number) conf.get(Config.TRANSACTIONAL_ZOOKEEPER_PORT)).intValue());
		stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, conf.get(Config.TRANSACTIONAL_ZOOKEEPER_ROOT));
		_state = new ZkState(stateConf);
		_connections = makeConnections(conf);
		// _connections = new DynamicPartitionConnections(_spoutConfig,
		// KafkaUtils.makeBrokerReader(conf, _spoutConfig));

		// using TransactionalState like this is a hack
		int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
		_coordinator = makeCoordinator(_connections, conf, _state, context.getThisTaskIndex(), totalTasks);
		registerMetrics(context);

	}

}
