package org.robotninjas.barge.log;

import com.google.common.base.Function;

public class G8 {

  public static <K, V> Function<K, V> fn(java.util.function.Function<K, V> f) {
    return new Function<K, V>() {
      @Override
      public V apply(K input) {
        return f.apply(input);
      }
    };
  }

}
