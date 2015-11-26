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
public class IntegerRange extends Range {


    private final int minIncl;
    private final int maxIncl;

    public final int min;
    public final int max;
    public final boolean minInclusive;
    public final boolean maxInclusive;

    /** Create a LongRange. */
    public IntegerRange(String label, int min, boolean minInclusive, int max, boolean maxInclusive) {
        super(label);
        this.min = min;
        this.max = max;
        this.minInclusive = minInclusive;
        this.maxInclusive = maxInclusive;

        if (!minInclusive && min != Long.MAX_VALUE) {
            min++;
        }

        if (!maxInclusive && max != Long.MIN_VALUE) {
            max--;
        }

        this.minIncl = min;
        this.maxIncl = max;
    }

    @Override
    public boolean accept(long value) {
        return value >= minIncl && value <= maxIncl;
    }
}
