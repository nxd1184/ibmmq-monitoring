package com.tsa.mqmonitor.service.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.ibm.mq.MQMessage;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.CMQCFC;
import com.ibm.mq.headers.pcf.PCFMessage;
import org.junit.jupiter.api.Test;

class PcfCodecTest {
  @Test
  void roundTripsInquireQueue() throws Exception {
    var req = new PCFMessage(CMQCFC.MQCMD_INQUIRE_Q);
    req.addParameter(CMQC.MQCA_Q_NAME, "APP.*");
    req.addParameter(CMQC.MQIA_Q_TYPE, CMQC.MQQT_LOCAL);
    var back = PcfCodec.decode(PcfCodec.encode(req));
    assertThat(back.getCommand()).isEqualTo(CMQCFC.MQCMD_INQUIRE_Q);
    assertThat(back.getStringParameterValue(CMQC.MQCA_Q_NAME).trim()).isEqualTo("APP.*");
    assertThat(back.getIntParameterValue(CMQC.MQIA_Q_TYPE)).isEqualTo(CMQC.MQQT_LOCAL);
  }

  @Test
  void isLastReadsControlFlag() throws Exception {
    var last = new PCFMessage(CMQCFC.MQCFT_RESPONSE, CMQCFC.MQCMD_INQUIRE_Q, 1, true);
    var more = new PCFMessage(CMQCFC.MQCFT_RESPONSE, CMQCFC.MQCMD_INQUIRE_Q, 1, false);
    assertThat(PcfCodec.isLast(PcfCodec.decode(PcfCodec.encode(last)))).isTrue();
    assertThat(PcfCodec.isLast(PcfCodec.decode(PcfCodec.encode(more)))).isFalse();
  }

  @Test
  void decodesLittleEndianReplyWithItsEncoding() throws Exception {
    var reply = new PCFMessage(CMQCFC.MQCFT_RESPONSE, CMQCFC.MQCMD_INQUIRE_Q, 1, true);
    reply.addParameter(CMQC.MQIA_CURRENT_Q_DEPTH, 3);
    var m = new MQMessage();
    m.encoding = CMQC.MQENC_INTEGER_REVERSED | CMQC.MQENC_DECIMAL_NORMAL | CMQC.MQENC_FLOAT_IEEE_REVERSED; // 546
    reply.write(m);
    byte[] bytes = new byte[m.getMessageLength()];
    m.seek(0);
    m.readFully(bytes);

    var back = PcfCodec.decode(bytes, 546);
    assertThat(PcfCodec.isLast(back)).isTrue();
    assertThat(back.getIntParameterValue(CMQC.MQIA_CURRENT_Q_DEPTH)).isEqualTo(3);
  }
}
