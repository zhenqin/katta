/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ivyft.katta.presto;

import com.facebook.presto.spi.block.SortOrder;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class KattaIndex {
    private final String name;
    private final List<KattaIndexKey> keys;
    private final boolean unique;


    public KattaIndex(String name, List<KattaIndexKey> keys, boolean unique) {
        this.name = name;
        this.keys = keys;
        this.unique = unique;
    }

    public String getName() {
        return name;
    }

    public List<KattaIndexKey> getKeys() {
        return keys;
    }

    public boolean isUnique() {
        return unique;
    }

    public static class KattaIndexKey {
        private final String name;
        private final Optional<SortOrder> sortOrder;
        private final Optional<String> type;

        public KattaIndexKey(String name, SortOrder sortOrder) {
            this(name, Optional.of(sortOrder), Optional.empty());
        }

        public KattaIndexKey(String name, String type) {
            this(name, Optional.empty(), Optional.of(type));
        }

        public KattaIndexKey(String name, Optional<SortOrder> sortOrder, Optional<String> type) {
            this.name = requireNonNull(name, "name is null");
            this.sortOrder = sortOrder;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public Optional<SortOrder> getSortOrder() {
            return sortOrder;
        }

        public Optional<String> getType() {
            return type;
        }
    }
}
