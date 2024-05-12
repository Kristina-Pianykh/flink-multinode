package com.huberlin.communication.addresses;

import com.huberlin.util.StringWrapper;
import org.apache.commons.lang3.StringUtils;

public final class TCPAddressString extends StringWrapper {
  public TCPAddressString(String the_string) {
    super(the_string);
  }

  public String getHost() {
    String first = StringUtils.substringBeforeLast(this.toString(), ":");
    if (first.startsWith("[") && first.endsWith("]")) // strip [ } (for ipv6 [ip]:port format)
    return first.substring(1, first.length() - 1);
    return first;
  }

  public int getPort() {
    return Integer.parseInt(StringUtils.substringAfterLast(this.toString(), ":"));
  }
}
