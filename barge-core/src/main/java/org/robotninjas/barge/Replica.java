/**
 * Copyright 2013 David Rusek <dave dot rusek at gmail dot com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.robotninjas.barge;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
@ThreadSafe
public class Replica {

  private final String key;
  private InetSocketAddress address;

  protected Replica(@Nonnull String key) {
    this.key = key;
  }

  @Nonnull
  public static Replica fromString(@Nonnull String key) {
      checkNotNull(key);
      return new Replica(key);
  }

  public synchronized SocketAddress address() {
    if (address == null) {
      try {
        HostAndPort hostAndPort = HostAndPort.fromString(key);
        InetAddress addr = InetAddress.getByName(hostAndPort.getHostText());
        InetSocketAddress saddr = new InetSocketAddress(addr, hostAndPort.getPort());
        this.address = saddr;
      } catch (UnknownHostException e) {
        throw Throwables.propagate(e);
      }
    }
    return address;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key);
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) {
      return true;
    }

    if (o instanceof Replica) {
      Replica other = (Replica) o;
      return Objects.equal(key, other.key);
    }

    return false;

  }

  @Nonnull
  @Override
  public String toString() {
    return key;
  }

  public String getKey() {
    return key;
  }
}
