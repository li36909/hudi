package org.apache.hudi.spark3.catalog;

import org.apache.spark.sql.connector.catalog.CatalogV2Implicits;
import org.apache.spark.sql.connector.catalog.Identifier;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The HoodieIdentifier.
 *
 * @since 2021/1/13
 */
public class HoodieIdentifier implements Identifier {

  private String[] namespace;
  private String name;

  public HoodieIdentifier(String[] namespace, String name) {
    this.namespace = namespace;
    this.name = name;
  }

  public HoodieIdentifier(Identifier identifier) {
    this(identifier.namespace(), identifier.name());
  }

  @Override
  public String[] namespace() {
    return namespace;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String toString() {
    return Stream.concat(Stream.of(namespace), Stream.of(name))
        .map(CatalogV2Implicits::quoteIfNeeded)
        .collect(Collectors.joining("."));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HoodieIdentifier that = (HoodieIdentifier) o;
    return Arrays.equals(namespace, that.namespace) && name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(namespace), name);
  }
}
