package com.tsa.mqmonitor.mq.pcf;

import static org.assertj.core.api.Assertions.assertThat;

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
}
