package com.ivyft.katta.lib.lucene.group;

import org.apache.hadoop.io.*;
import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.FloatRange;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.Range;
import org.apache.solr.common.params.FacetParams;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 14-1-9
 * Time: 下午7:37
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public abstract class GroupCollectorFactory {


    /**
     * 线程安全，线程本地化
     */
    final ThreadLocal<DateFormat> threadLocal = new ThreadLocal<DateFormat>();


    /**
     * 把Java基本类型转换成Hadoop IO Writable对应的类型
     *
     * @param v 基本类型
     * @return 返回Hadoop Writable对应的类型
     *
     */
    public abstract <T extends Writable> T convert(Object v);


    /**
     * 解析数值型区间。
     *
     * @param rangeField 数值字段
     * @param startString 开始值
     * @param endString 结束值
     * @param gapString 步进值
     * @return 返回生成的区间组成的List
     */
    public abstract List<Range> getNumberRanges(String rangeField,
                                                String startString,
                                                String endString,
                                                String gapString);




    /**
     * 解析数值型区间。
     *
     * @param rangeField 数值字段
     * @param dateRange 开始值
     * @return 返回生成的区间组成的List
     */
    public abstract List<Range> getNumberRanges(String rangeField,
                                                String[] dateRange);


    /**
     * 解析等值日期区间
     *
     * @param rangeField 日期字段
     * @param startString 开始时间，时间字符串
     * @param endString 结束时间，时间字符串
     * @param gapString 布增值，每月，每日，小时
     * @return 返回日期各个区间
     */
    public List<Range> getDateRanges(String rangeField,
                                     String startString,
                                     String endString,
                                     String gapString) {
        DateFormat format = threadLocal.get();
        if(format == null) {
            format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            threadLocal.set(format);
        }

        List<Range> list = new LinkedList<Range>();
        try {
            Date startDate = format.parse(startString);
            Date endDate = format.parse(endString);
            DateRangeFactory.TimeRange<Long> rangeList =
                    DateRangeFactory.stepDateRange(
                            startDate,
                            endDate,
                            gapString);
            for (Long[] range : rangeList) {
                list.add(new LongRange(String.valueOf(range[0]),
                        range[0], true, range[1], false));
            }
        } catch (ParseException e) {
            throw new IllegalStateException(e);
        }

        return list;
    }


    /**
     * 解析不等值日期区间
     *
     * @param rangeField 日期字段
     * @param dateRange 不等值时间区间，时间字符串
     * @return 返回日期各个区间
     */
    public List<Range> getDateRanges(String rangeField,
                                     String[] dateRange) {
        DateFormat format = threadLocal.get();
        if(format == null) {
            format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            threadLocal.set(format);
        }

        if(dateRange.length == 0) {
            throw new IllegalArgumentException(FacetParams.FACET_QUERY + " 参数长度必须大于0.");
        }
        String rang = dateRange[0];

        DateRangeFactory.TimeRange<Long> rangeList = null;
        if(rang.matches("(\\[)?\\d+\\s+TO\\s+\\d+(\\])?")) {
            rangeList = new DateRangeFactory.UnFormLongRange(rangeField, dateRange);
        } else {
            rangeList = new DateRangeFactory.UnFormDateRange(rangeField, format, dateRange);
        }

        List<Range> list = new LinkedList<Range>();
        for (Long[] range : rangeList) {
            list.add(new LongRange(String.valueOf(range[0]),
                    range[0], true, range[1], false));
        }
        return list;
    }


    /**
     * 静态工厂方法
     * @param type 类型
     *             震秦震秦在
     * @return 返回GroupCollectorFactory子类
     */
    public static GroupCollectorFactory getInstance(Writable type) {
        if(type instanceof IntWritable) {
            return new IntCollectorFactory();
        } else if(type instanceof LongWritable) {
            return new LongCollectorFactory();
        } else if(type instanceof Text) {
            return new TextCollectorFactory();
        } else if(type instanceof DoubleWritable) {
            return new DoubleCollectorFactory();
        } else if(type instanceof FloatWritable) {
            return new FloatCollectorFactory();
        } else if(type instanceof BooleanWritable) {
            return new BooleanCollectorFactory();
        }
        return new NullCollectorFactory();
    }



    public static class IntCollectorFactory extends GroupCollectorFactory {

        @Override
        public IntWritable convert(Object v) {
            if(v == null) {
                return null;
            }
            if(v instanceof String) {
                return new IntWritable(Integer.valueOf((String)v));
            }
            return new IntWritable((Integer)v);
        }

        @Override
        public List<Range> getNumberRanges(String rangeField,
                                           String startString,
                                           String endString,
                                           String gapString) {
            int start = Integer.valueOf(startString);
            int end = Integer.valueOf(endString);
            int gap = Integer.valueOf(gapString);
            List<Range> list = new LinkedList<Range>();
            int s = start;
            while (true) {
                int e = s + gap;
                list.add(
                        new LongRange(String.valueOf(s),
                                s, true, Math.min(e, end), false));
                s = e;
                if(e >= end) {
                    break;
                }
            }
            return list;
        }


        public List<Range> getNumberRanges(String rangeField, String[] dateRange) {
            DateRangeFactory.UnFormLongRange doubleRange =
                    new DateRangeFactory.UnFormLongRange(rangeField, dateRange);
            List<Range> rangeList = new LinkedList<Range>();
            for (Long[] r : doubleRange) {
                rangeList.add(new LongRange(String.valueOf(r[0]), r[0],
                        true, r[1], false));
            }
            return rangeList;
        }
    }


    public static class LongCollectorFactory extends GroupCollectorFactory {

        @Override
        public LongWritable convert(Object v) {
            if(v == null) {
                return null;
            }
            if(v instanceof String) {
                return new LongWritable(Long.valueOf((String)v));
            }
            return new LongWritable((Long)v);
        }

        @Override
        public List<Range> getNumberRanges(String rangeField,
                                           String startString,
                                           String endString,
                                           String gapString) {
            long start = Long.valueOf(startString);
            long end = Long.valueOf(endString);
            long gap = Long.valueOf(gapString);
            List<Range> list = new LinkedList<Range>();
            long s = start;
            while (true) {
                long e = s + gap;
                list.add(new LongRange(String.valueOf(s), s, true,
                        Math.min(e, end), false));
                s = e;
                if(e >= end) {
                    break;
                }
            }
            return list;
        }



        public List<Range> getNumberRanges(String rangeField,
                                           String[] dateRange) {
            DateRangeFactory.UnFormLongRange doubleRange =
                    new DateRangeFactory.UnFormLongRange(rangeField, dateRange);
            List<Range> rangeList = new LinkedList<Range>();
            for (Long[] r : doubleRange) {
                rangeList.add(new LongRange(String.valueOf(r[0]), r[0],
                        true, r[1], false));
            }
            return rangeList;
        }
    }


    public static class DoubleCollectorFactory extends GroupCollectorFactory {

        @Override
        public DoubleWritable convert(Object v) {
            if(v == null) {
                return null;
            }
            if(v instanceof String) {
                return new DoubleWritable(Double.valueOf((String)v));
            }
            return new DoubleWritable((Double)v);
        }


        @Override
        public List<Range> getNumberRanges(String rangeField,
                                           String startString,
                                           String endString,
                                           String gapString) {
            double start = Double.valueOf(startString);
            double end = Long.valueOf(endString);
            double gap = Long.valueOf(gapString);
            List<Range> list = new LinkedList<Range>();
            double s = start;
            while (true) {
                double e = s + gap;
                list.add(new DoubleRange(String.valueOf(s), s, true,
                        Math.min(e, end), false));
                s = e;
                if(e >= end) {
                    break;
                }
            }
            return list;
        }



        public List<Range> getNumberRanges(String rangeField,
                                           String[] dateRange) {
            DateRangeFactory.UnFormDoubleRange doubleRange =
                    new DateRangeFactory.UnFormDoubleRange(rangeField, dateRange);
            List<Range> rangeList = new LinkedList<Range>();
            for (Double[] r : doubleRange) {
                rangeList.add(new DoubleRange(String.valueOf(r[0]), r[0],
                        true, r[1], false));
            }
            return rangeList;
        }
    }


    public static class FloatCollectorFactory extends GroupCollectorFactory {

        @Override
        public FloatWritable convert(Object v) {
            if(v == null) {
                return null;
            }
            if(v instanceof String) {
                return new FloatWritable(Float.valueOf((String)v));
            }
            return new FloatWritable((Float)v);
        }


        @Override
        public List<Range> getNumberRanges(String rangeField,
                                           String startString,
                                           String endString,
                                           String gapString) {
            float start = Float.valueOf(startString);
            float end = Float.valueOf(endString);
            float gap = Float.valueOf(gapString);
            List<Range> list = new LinkedList<Range>();
            float s = start;
            while (true) {
                float e = s + gap;
                list.add(new FloatRange(String.valueOf(s), s, true,
                        Math.min(e, end), false));
                s = e;
                if(e >= end) {
                    break;
                }
            }
            return list;
        }



        public List<Range> getNumberRanges(String rangeField,
                                           String[] dateRange) {
            DateRangeFactory.UnFormFloatRange doubleRange =
                    new DateRangeFactory.UnFormFloatRange(rangeField, dateRange);
            List<Range> rangeList = new LinkedList<Range>();
            for (Float[] r : doubleRange) {
                rangeList.add(new FloatRange(String.valueOf(r[0]), r[0],
                        true, r[1], false));
            }
            return rangeList;
        }
    }



    public static class TextCollectorFactory extends GroupCollectorFactory {

        @Override
        public Text convert(Object v) {
            if(v == null) {
                return null;
            }
            if(v instanceof String) {
                return new Text((String)v);
            }
            return new Text(String.valueOf(v));
        }


        @Override
        public List<Range> getNumberRanges(String rangeField,
                                           String startString,
                                           String endString,
                                           String gapString) {
            throw new IllegalStateException("string 类型字段，不支持区间统计。");
        }


        public List<Range> getNumberRanges(String rangeField,
                                           String[] dateRange) {
            throw new IllegalStateException("string 类型字段，不能分配统计区间。");
        }
    }




    public static class BooleanCollectorFactory extends GroupCollectorFactory {

        @Override
        public BooleanWritable convert(Object v) {
            if(v == null) {
                return null;
            }
            if(v instanceof String) {
                return new BooleanWritable(Boolean.valueOf((String)v));
            }
            return new BooleanWritable((Boolean)v);
        }

        @Override
        public List<Range> getNumberRanges(String rangeField,
                                           String startString,
                                           String endString,
                                           String gapString) {
            throw new IllegalStateException("boolean 类型字段，不支持区间统计。");
        }



        public List<Range> getNumberRanges(String rangeField,
                                           String[] dateRange) {
            throw new IllegalStateException("boolean 类型字段，不能分配统计区间。");
        }
    }


    public static class NullCollectorFactory extends GroupCollectorFactory {

        @Override
        public NullWritable convert(Object v) {
            return NullWritable.get();
        }


        @Override
        public List<Range> getNumberRanges(String rangeField,
                                           String startString,
                                           String endString,
                                           String gapString) {
            throw new IllegalStateException("错误的类型，不支持区间统计。");
        }



        public List<Range> getNumberRanges(String rangeField,
                                           String[] dateRange) {
            throw new IllegalStateException("错误的类型，不支持区间统计。");
        }
    }
}
