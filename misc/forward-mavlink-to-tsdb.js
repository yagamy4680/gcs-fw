#!/usr/bin/env node
/**
 * Copyright (c) 2019-2025 T2T Inc. All rights reserved
 * 
 *  https://www.t2t.io
 *  https://tic-tac-toe.io
 * 
 * Taipei, Taiwan
 */
'use strict';

const fs = require('fs');
const colors = require('@colors/colors');
const yargs = require('yargs');
const io = require('socket.io-client');
const { MavLinkPacketSplitter, MavLinkPacketParser, MavLinkPacketRegistry, minimal, common, ardupilotmega } = require('node-mavlink');
const { Writable, PassThrough } = require('stream');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');

/**
 * 1. Connect to WebSocket server as client, with given `host` and `port`. Note, the channel is always `/serial`.
 * 2. Listen to `from_serial`, `to_serial`, and `status` events, and dump them to console.
 * 3. The `host` and `port` are given as command line arguments, parsed by yargs library.
 */


const MAVLINK_MSG_HANDLER_MAP = {

  // {
  //   "timeUsec": 96098420,
  //   "xacc": -215,
  //   "yacc": -19,
  //   "zacc": -1006,
  //   "xgyro": 0,
  //   "ygyro": -5,
  //   "zgyro": 0,
  //   "xmag": 331,
  //   "ymag": 336,
  //   "zmag": 0,
  //   "id": 0,
  //   "temperature": 3058
  // }
  RAW_IMU: {
    process_context: (data, context) => {
      let { id } = data;
      context.imu = `imu${id}`;
      return context;
    },
    process_data: (data) => data,
    exclude_fields: ['timeUsec', 'id'],
  },

  // {
  //   "timeBootMs": 2251603,
  //   "xacc": -218,
  //   "yacc": -13,
  //   "zacc": -974,
  //   "xgyro": -7,
  //   "ygyro": -26,
  //   "zgyro": 1,
  //   "xmag": 233,
  //   "ymag": 308,
  //   "zmag": -6,
  //   "temperature": 3821
  // }
  SCALED_IMU2: {
    process_data: (data) => data,
    exclude_fields: ['timeBootMs'],
  },

  // {
  //   "timeBootMs": 2251603,
  //   "pressAbs": 1002.8594970703125,
  //   "pressDiff": 0,
  //   "temperature": 3662,
  //   "temperaturePressDiff": 0
  // }
  SCALED_PRESSURE: {
    process_data: (data) => data,
    exclude_fields: ['timeBootMs'],
  },

  // {
  //   "timeUsec": 2251613517,
  //   "vibrationX": 0.02015392668545246,
  //   "vibrationY": 0.02397291176021099,
  //   "vibrationZ": 0.031107155606150627,
  //   "clipping0": 0,
  //   "clipping1": 0,
  //   "clipping2": 0
  // }
  VIBRATION: {
    process_data: (data) => data,
    exclude_fields: ['timeUsec'],
  },

  // {
  //   "Vcc": 4281,
  //   "Vservo": 4322,
  //   "flags": 0
  // }
  POWER_STATUS: {
    process_data: (data) => data,
  },

  // {
  //   "timeUsec": 3073101400,
  //   "port": 0,
  //   "servo1Raw": 1000,
  //   "servo2Raw": 1000,
  //   "servo3Raw": 1000,
  //   "servo4Raw": 1000,
  //   "servo5Raw": 0,
  //   "servo6Raw": 0,
  //   "servo7Raw": 0,
  //   "servo8Raw": 0,
  //   "servo9Raw": 0,
  //   "servo10Raw": 0,
  //   "servo11Raw": 0,
  //   "servo12Raw": 0,
  //   "servo13Raw": 0,
  //   "servo14Raw": 0,
  //   "servo15Raw": 0,
  //   "servo16Raw": 0
  // }
  SERVO_OUTPUT_RAW: {
    process_data: (data) => data,
    exclude_fields: ['timeUsec', 'port'],
  },

  // {
  //   "id": 0,
  //   "batteryFunction": 0,
  //   "type": 0,
  //   "temperature": 32767,
  //   "voltages": [
  //     22,
  //     65535,
  //     65535,
  //     65535,
  //     65535,
  //     65535,
  //     65535,
  //     65535,
  //     65535,
  //     65535
  //   ],
  //   "currentBattery": 7,
  //   "currentConsumed": 59,
  //   "energyConsumed": 0,
  //   "batteryRemaining": 98,
  //   "timeRemaining": 0,
  //   "chargeState": 1,
  //   "voltagesExt": [
  //     0,
  //     0,
  //     0,
  //     0
  //   ],
  //   "mode": 0,
  //   "faultBitmask": 0
  // }
  BATTERY_STATUS: {
    process_context: (data, context) => {
      let { id } = data;
      context.battery = `battery${id}`;
      return context;
    },
    process_data: (data) => {
      let { voltages } = data;
      data['voltage0'] = voltages[0];
      data['voltage1'] = voltages[1];
      return data;
    },
    exclude_fields: ['id', 'voltages', 'voltagesExt'],
  },

  // {
  //   "roll": 0.017456967383623123,
  //   "pitch": -0.20928500592708588,
  //   "yaw": -0.8029261827468872,
  //   "altitude": -2.679999828338623,
  //   "lat": 0,
  //   "lng": 0
  // }
  AHRS2: {
    process_data: (data) => data,
  },


  // {
  //   "timeBootMs": 3433111,
  //   "roll": 0.017750082537531853,
  //   "pitch": -0.21112160384655,
  //   "yaw": -0.8043689131736755,
  //   "rollspeed": -0.0009689554572105408,
  //   "pitchspeed": 0.0003054991830140352,
  //   "yawspeed": 0.0004362011095508933
  // }
  ATTITUDE: {
    process_data: (data) => data,
    exclude_fields: ['timeBootMs'],
  }
};


// create a registry of mappings between a message id and a data class
const REGISTRY = {
  ...minimal.REGISTRY,
  ...common.REGISTRY,
  ...ardupilotmega.REGISTRY,
};

class MavLinkDumper {
  constructor(systemId = 255, componentId = 190, writeClient = null, name = 'GCS', verbose = false) {
    this.systemId = systemId;
    this.componentId = componentId;
    this.writeClient = writeClient;
    this.name = name;
    this.verbose = verbose;
    this.ignore_message_map = {};
    this.uart = 'unknown';
    this.tx_splitter = new MavLinkPacketSplitter();
    this.tx_parser = new MavLinkPacketParser();
    this.rx_splitter = new MavLinkPacketSplitter();
    this.rx_parser = new MavLinkPacketParser();
    this.tx_splitter.pipe(this.tx_parser);
    this.rx_splitter.pipe(this.rx_parser);
    this.tx_splitter.on('data', (packet) => this.on_tx_packet(packet));
    this.rx_splitter.on('data', (packet) => this.on_rx_packet(packet));
    this.tx_parser.on('data', (message) => this.on_tx_message(message));
    this.rx_parser.on('data', (message) => this.on_rx_message(message));
    this.timer = setInterval(async () => await this.flush(), 5 * 1000);
  }

  set_ignore_messages(ignore_message_map) {
    this.ignore_message_map = ignore_message_map;
  }

  set_uart(uart) {
    this.uart = uart;
  }

  feed_tx(data) {
    this.tx_splitter.write(data);
  }

  feed_rx(data) {
    this.rx_splitter.write(data);
  }

  on_tx_packet(packet) {

  }

  on_rx_packet(packet) {

  }

  process_metric(data, clazz, direction, ts = Date.now()) {
    let { writeClient } = this;
    if (!writeClient) return false;
    const metric = `mavlink_${clazz.MSG_NAME}`;
    const handler = MAVLINK_MSG_HANDLER_MAP[clazz.MSG_NAME];
    if (!handler) return false;
    let context = {};
    let new_data = { ...data };
    if (handler.process_context) {
      context = handler.process_context(new_data, context);
    }
    // console.log(`[${direction}] ${metric} @ ${this.name}`);
    // console.log(context);
    if (handler.process_data) {
      new_data = handler.process_data(new_data);
    }
    // console.log(new_data);
    const point = new Point(metric)
      .timestamp(ts)
      .tag('gcs', this.name)
      .tag('category', 'mavlink');
    for (let key in context) {
      point.tag(key, context[key]);
    }
    for (let key in new_data) {
      let value = new_data[key];
      if (typeof value === 'number') {
        point.floatField(key, value);
      } else if (typeof value === 'bigint') {
        point.intField(key, Number(value));
      } else if (typeof value === 'string') {
        point.stringField(key, value);
      }
    }
    writeClient.writePoint(point);
    return true;
  }

  async flush() {
    try {
      this.writeClient.flush();
    } catch (error) {
      console.error('Error flushing write client:', error);
    }
  }

  dump_message(message, direction = '->', color = 'white') {
    let { uart, verbose, ignore_message_map } = this;
    const now = Date.now();
    const clazz = REGISTRY[message.header.msgid]
    if (clazz) {
      let ts = (new Date()).toISOString().substring(11);
      let { MSG_ID, MAGIC_NUMBER, MSG_NAME } = clazz;
      if (ignore_message_map[MSG_NAME]) {
        return; // ignore this message
      }
      MSG_ID = MSG_ID.toString(16).toUpperCase().padStart(4, ' ');
      MAGIC_NUMBER = MAGIC_NUMBER.toString(16).toUpperCase().padStart(4, ' ');
      const data = message.protocol.data(message.payload, clazz);
      let processed = false;
      try {
        processed = this.process_metric(data, clazz, direction, now);
      } catch (error) {
        console.error('Error processing metric:', error);
        console.dir(error);
      }
      // let sign = processed ? '✔' : '✘';
      let text = JSON.stringify(data, (key, value) => typeof value === "bigint" ? Number(value) : value);
      text = processed ? text.green : text[color];
      let tokens = [
        ts.gray,
        uart.blue,
        direction,
        MSG_NAME.padStart(22).yellow,
        `[${MSG_ID.cyan}]`,
        text,
      ];
      if (!processed || (processed && verbose)) {
        console.log(tokens.join(' '));
      }
    }
  }

  on_tx_message(message) {
    this.dump_message(message, '<-', 'red');
  }

  on_rx_message(message) {
    this.dump_message(message, '->');
  }
}


// Parse command line arguments
const argv = yargs()
  .option('host', {
    alias: 'h',
    describe: 'WebSocket server host',
    demandOption: true,
    type: 'string',
    default: 'localhost'
  })
  .option('port', {
    alias: 'p',
    describe: 'WebSocket server port',
    demandOption: true,
    type: 'number'
  })
  .option('name', {
    alias: 'n',
    describe: 'The name of GCS',
    demandOption: true,
    type: 'string'
  })
  .option('verbose', {
    alias: 'v',
    describe: 'Enable verbose logging',
    demandOption: false,
    type: 'boolean',
    default: false
  })
  .option('exclude_conf', {
    alias: 'e',
    describe: 'The path to the MAVLink ignore messages config file (CONF format)',
    demandOption: false,
    type: 'string'
  })
  .help()
  .parse(process.argv.slice(2));

const TOKEN = 'FGlr6C1xcarf6fh__qSFaCG7reLfEag3t3_ETAbY4ikmh_rnno4d2xc3CDhCd9-ItmeOi_IAvR87bj3x1AkXWw==';
const URL = 'https://tsdb.t2t.io';
const client = new InfluxDB({ url: URL, token: TOKEN });
const ORG = `f908686d9d548c82`; // t2t organization
const BUCKET = `tt-dev-90d`;
let writeClient = client.getWriteApi(ORG, BUCKET, 'ms');
const dumper = new MavLinkDumper(255, 255, writeClient, argv.name, argv.verbose);

if (argv.exclude_conf) {
  try {
    let ignore_message_map = {};
    let content = fs.readFileSync(argv.exclude_conf, 'utf-8');
    let lines = content.split('\n');
    for (let line of lines) {
      line = line.trim();
      if (line.startsWith('#') || line.length === 0) continue;
      ignore_message_map[line] = true;
    }
    dumper.set_ignore_messages(ignore_message_map);
  } catch (error) {
    console.error('Error reading exclude config file:', error);
  }
}


// Build WebSocket URL
const url = `ws://${argv.host}:${argv.port}/serial`;

// Connect to WebSocket server
const socket = io(url);

let uart = 'unknown';

// Listen and dump events to console
socket.on('connect', () => {
  console.log(`Connected to ${url}`);
});

socket.on('from_serial', (data) => {
  let { chunk } = data;
  let bytes = Buffer.from(chunk, 'base64');
  dumper.feed_rx(bytes);
});

socket.on('to_serial', (data) => {
  let { chunk } = data;
  let bytes = Buffer.from(chunk, 'base64');
  dumper.feed_tx(bytes);
});

socket.on('status', (data) => {
  // console.log('[status]', data);
  uart = data.uart || uart;
  dumper.set_uart(uart);
});

socket.on('disconnect', () => {
  console.log('Disconnected');
});

socket.on('connect_error', (err) => {
  console.error('Connection error:', err.message);
});