package com.ivyft.katta.lib.lucene.range;

import org.apache.lucene.facet.range.Range;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 14-1-10
 * Time: 下午5:10
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class ShortRange extends Range {


    private final short minIncl;
    private final short maxIncl;

    public final short min;
    public final short max;
    public final boolean minInclusive;
    public final boolean maxInclusive;

    /** Create a LongRange. */
    public ShortRange(String label, short min, boolean minInclusive, short max, boolean maxInclusive) {
        super(label);
        this.min = min;
        this.max = max;
        this.minInclusive = minInclusive;
        this.maxInclusive = maxInclusive;

        if (!minInclusive && min != Short.MAX_VALUE) {
            min++;
        }

        if (!maxInclusive && max != Short.MIN_VALUE) {
            max--;
        }

        this.minIncl = min;
        this.maxIncl = max;
    }

    @Override
    public boolean accept(long value) {
        float floatValue = (short) value;
        return floatValue >= minIncl && floatValue <= maxIncl;
    }
}
