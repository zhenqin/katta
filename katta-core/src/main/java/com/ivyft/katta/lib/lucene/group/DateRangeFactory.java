package com.ivyft.katta.lib.lucene.group;


import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 14-1-10
 * Time: 下午9:35
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class DateRangeFactory {


    public static enum TimeUnit {


        YEAR, MONTH, DATE, HOUR, MINUTE, SECOND;


        public static TimeUnit get(String v) {
            return valueOf(v);
        }
    }


    public static TimeRange<Long> stepDateRange(Date startDate,
                                          Date endDate,
                                          String gapString) {
        Matcher m = Pattern.compile("\\d+").matcher(gapString);
        int gap = 0;
        if(m.find()) {
            gap = Integer.valueOf(m.group());
        }
        if(gap < 1) {
            throw new IllegalArgumentException("时间间隔参数："+ gapString +
                    " 不正确，不能解析出正确的区间间隔。");
        }
        String u = gapString.replaceAll("\\d+", "");
        TimeUnit timeUnit = TimeUnit.get(u);
        if(timeUnit == null) {
            throw new IllegalArgumentException("时间间隔参数："+ u +
                    " 不正确，支持的类型为：YEAR, MONTH, DATE, HOUR, MINUTE, SECOND");
        }
        switch (timeUnit) {
            case YEAR:
                return new YearRange(startDate, endDate, gap);
            case MONTH:
                return new MonthRange(startDate, endDate, gap);
            case DATE:
                return new DateRange(startDate, endDate, gap);
            case HOUR:
                return new HourRange(startDate, endDate, gap);
            case MINUTE:
                return new MinuteRange(startDate, endDate, gap);
            case SECOND:
                return new SecondRange(startDate, endDate, gap);
            default:
                throw new IllegalStateException("未知的时间区间类型：" + timeUnit);
        }
    }



    public static abstract class TimeRange<T>
            implements Iterator<T[]>, Iterable<T[]> {

    }

    static class YearRange extends TimeRange<Long> {

        private DateTime start;

        private DateTime end;

        private int gap;

        YearRange(Date startDate, Date endDate, int gap) {
            this.start = new DateTime(startDate);
            this.end = new DateTime(endDate);
            this.gap = gap;
        }


        @Override
        public boolean hasNext() {
            return start.isBefore(end);
        }

        @Override
        public Long[] next() {
            if(start.isAfter(end)) {
                return null;
            }

            int y = end.getYear() - start.getYear();
            if(y > 0) {
                //不论够不够一年，但是跨年了
                DateTime tmp = start.withDate(start.getYear(), 1, 1).plusYears(gap).withTime(0, 0, 0, 0);

                if(tmp.isAfter(end)) {
                    Long[] range = new Long[]{ start.getMillis(),
                            end.getMillis() };
                    start = tmp;
                    return range;
                }

                Long[] range = new Long[]{ start.getMillis(),
                        tmp.getMillis() };
                start = tmp;
                return range;
            } else {
                //明显不够一年的时间
                Long[] range = new Long[]{ start.getMillis(),
                        end.getMillis() };
                start = end;
                return range;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove()方法不被支持。");
        }

        @Override
        public Iterator<Long[]> iterator() {
            return this;
        }
    }



    static class MonthRange extends TimeRange<Long> {

        private DateTime start;

        private DateTime end;

        private int gap;

        MonthRange(Date startDate, Date endDate, int gap) {
            this.start = new DateTime(startDate);
            this.end = new DateTime(endDate);
            this.gap = gap;
        }


        @Override
        public boolean hasNext() {
            return start.isBefore(end);
        }

        @Override
        public Long[] next() {
            if(start.isAfter(end)) {
                return null;
            }

            int y = end.getYear() - start.getYear();
            int m = end.getMonthOfYear() - start.getMonthOfYear() + y * 12;
            if(y + m > 0) {
                //不论够不够一月，但是跨月了
                DateTime tmp = start.withDayOfMonth(1).plusMonths(gap).withTime(0, 0, 0, 0);


                if(tmp.isAfter(end)) {
                    Long[] range = new Long[]{ start.getMillis(),
                            end.getMillis() };
                    start = tmp;
                    return range;
                }

                Long[] range = new Long[]{ start.getMillis(),
                        tmp.getMillis() };
                start = tmp;
                return range;
            } else {
                //明显不够一月的时间
                Long[] range = new Long[]{ start.getMillis(),
                        end.getMillis() };
                start = end;
                return range;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove()方法不被支持。");
        }

        @Override
        public Iterator<Long[]> iterator() {
            return this;
        }
    }


    static class DateRange extends TimeRange<Long> {

        private DateTime start;

        private DateTime end;

        private int gap;

        DateRange(Date startDate, Date endDate, int gap) {
            this.start = new DateTime(startDate);
            this.end = new DateTime(endDate);
            this.gap = gap;
        }


        @Override
        public boolean hasNext() {
            return start.isBefore(end);
        }

        @Override
        public Long[] next() {
            if(start.isAfter(end)) {
                return null;
            }

            long s = end.getMillis() - start.getMillis();
            if(s > 1000L * 60 * 60 * 24) {
                //不论够不够一天，但是跨年了
                DateTime tmp = start.withTime(0, 0, 0, 0).plusDays(gap);

                if(tmp.isAfter(end)) {
                    Long[] range = new Long[]{ start.getMillis(),
                            end.getMillis() };
                    start = tmp;
                    return range;
                }

                Long[] range = new Long[]{ start.getMillis(),
                        tmp.getMillis() };
                start = tmp;
                return range;
            } else {
                //明显不够一天的时间
                Long[] range = new Long[]{ start.getMillis(),
                        end.getMillis() };
                start = end;
                return range;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove()方法不被支持。");
        }

        @Override
        public Iterator<Long[]> iterator() {
            return this;
        }
    }


    static class HourRange extends TimeRange<Long> {

        private DateTime start;

        private DateTime end;

        private int gap;

        HourRange(Date startDate, Date endDate, int gap) {
            this.start = new DateTime(startDate);
            this.end = new DateTime(endDate);
            this.gap = gap;
        }


        @Override
        public boolean hasNext() {
            return start.isBefore(end);
        }

        @Override
        public Long[] next() {
            if(start.isAfter(end)) {
                return null;
            }

            long s = end.getMillis() - start.getMillis();
            if(s > 1000L * 60 * 60) {
                //不论够不够一天，但是跨年了
                DateTime tmp = start.withTime(start.getHourOfDay(), 0, 0, 0).plusHours(gap);
                if(tmp.isAfter(end)) {
                    Long[] range = new Long[]{ start.getMillis(),
                            end.getMillis() };
                    start = tmp;
                    return range;
                }

                Long[] range = new Long[]{ start.getMillis(),
                        tmp.getMillis() };
                start = tmp;
                return range;
            } else {
                //明显不够一天的时间
                Long[] range = new Long[]{ start.getMillis(),
                        end.getMillis() };
                start = end;
                return range;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove()方法不被支持。");
        }

        @Override
        public Iterator<Long[]> iterator() {
            return this;
        }
    }



    static class MinuteRange extends TimeRange<Long> {

        private DateTime start;

        private DateTime end;

        private int gap;

        MinuteRange(Date startDate, Date endDate, int gap) {
            this.start = new DateTime(startDate);
            this.end = new DateTime(endDate);
            this.gap = gap;
        }


        @Override
        public boolean hasNext() {
            return start.isBefore(end);
        }

        @Override
        public Long[] next() {
            if(start.isAfter(end)) {
                return null;
            }

            long s = end.getMillis() - start.getMillis();
            if(s > 1000L * 60) {
                //不论够不够一天，但是跨年了
                DateTime tmp = start.withTime(start.getHourOfDay(),
                        start.getMinuteOfHour(), 0, 0).plusMinutes(gap);

                if(tmp.isAfter(end)) {
                    Long[] range = new Long[]{ start.getMillis(),
                            end.getMillis() };
                    start = tmp;
                    return range;
                }

                Long[] range = new Long[]{ start.getMillis(),
                        tmp.getMillis() };
                start = tmp;
                return range;
            } else {
                //明显不够一天的时间
                Long[] range = new Long[]{ start.getMillis(),
                        end.getMillis() };
                start = end;
                return range;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove()方法不被支持。");
        }

        @Override
        public Iterator<Long[]> iterator() {
            return this;
        }
    }


    static class SecondRange extends TimeRange<Long> {

        private DateTime start;

        private DateTime end;

        private int gap;

        SecondRange(Date startDate, Date endDate, int gap) {
            this.start = new DateTime(startDate);
            this.end = new DateTime(endDate);
            this.gap = gap;
        }


        @Override
        public boolean hasNext() {
            return start.isBefore(end);
        }

        @Override
        public Long[] next() {
            if(start.isAfter(end)) {
                return null;
            }

            long s = end.getMillis() - start.getMillis();
            if(s > 1000L) {
                //不论够不够一天，但是跨年了
                DateTime tmp = start.withTime(start.getHourOfDay(),
                        start.getMinuteOfHour(), start.getSecondOfMinute(), 0).plusSeconds(gap);

                if(tmp.isAfter(end)) {
                    Long[] range = new Long[]{ start.getMillis(),
                            end.getMillis() };
                    start = tmp;
                    return range;
                }

                Long[] range = new Long[]{ start.getMillis(),
                        tmp.getMillis() };
                start = tmp;
                return range;
            } else {
                //明显不够一天的时间
                Long[] range = new Long[]{ start.getMillis(),
                        end.getMillis() };
                start = end;
                return range;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove()方法不被支持。");
        }

        @Override
        public Iterator<Long[]> iterator() {
            return this;
        }
    }



    public static class UnFormDateRange extends TimeRange<Long> {


        String rangeField;
        String[] dateRange;
        DateFormat format;
        private AtomicInteger index = new AtomicInteger(0);

        public UnFormDateRange(String rangeField, DateFormat format, String[] dateRange) {
            this.rangeField = rangeField;
            this.format = format;
            this.dateRange = dateRange;
        }


        @Override
        public boolean hasNext() {
            if(dateRange.length > index.get()) {
                return true;
            }
            return false;
        }

        @Override
        public Long[] next() {
            String range = dateRange[index.get()];
            String[] r = range.split("TO");
            String s = StringUtils.trim(r[0]);
            String e = StringUtils.trim(r[1]);
            int sint = s.indexOf("[");
            int eint = e.indexOf("]");

            sint = sint < 0 ? 0 : sint + 1;
            eint = eint < 0 ? e.length() : eint;

            String start = StringUtils.trim(s.substring(sint));
            String end = StringUtils.trim(e.substring(0, eint));
            index.incrementAndGet();
            try {
                return new Long[]{ format.parse(start).getTime(), format.parse(end).getTime() };
            } catch (ParseException ex) {
                throw new IllegalArgumentException("非法的日期区间：" + r[0] + " ，不能解析。");
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove()方法不被支持。");
        }

        @Override
        public Iterator<Long[]> iterator() {
            return this;
        }
    }



    public static class UnFormLongRange extends TimeRange<Long> {


        String rangeField;
        String[] dateRange;

        private AtomicInteger index = new AtomicInteger(0);

        public UnFormLongRange(String rangeField, String[] dateRange) {
            this.rangeField = rangeField;
            this.dateRange = dateRange;
        }


        @Override
        public boolean hasNext() {
            if(dateRange.length > index.get()) {
                return true;
            }
            return false;
        }

        @Override
        public Long[] next() {
            String range = dateRange[index.get()];
            String[] r = range.split("TO");
            String s = StringUtils.trim(r[0]);
            String e = StringUtils.trim(r[1]);
            int sint = s.indexOf("[");
            int eint = e.indexOf("]");

            sint = sint < 0 ? 0 : sint + 1;
            eint = eint < 0 ? e.length() : eint;

            String start = StringUtils.trim(s.substring(sint));
            String end = StringUtils.trim(e.substring(0, eint));
            index.incrementAndGet();
            try {
                return new Long[]{ Long.valueOf(start), Long.valueOf(end) };
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("非法的数值区间：" + r[0] + " ，不能解析。");
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove()方法不被支持。");
        }

        @Override
        public Iterator<Long[]> iterator() {
            return this;
        }
    }



    public static class UnFormIntegerRange extends TimeRange<Long> {


        String rangeField;
        String[] dateRange;

        private AtomicInteger index = new AtomicInteger(0);

        public UnFormIntegerRange(String rangeField, String[] dateRange) {
            this.rangeField = rangeField;
            this.dateRange = dateRange;
        }


        @Override
        public boolean hasNext() {
            if(dateRange.length > index.get()) {
                return true;
            }
            return false;
        }

        @Override
        public Long[] next() {
            String range = dateRange[index.get()];

            String[] r = range.split("TO");
            String s = StringUtils.trim(r[0]);
            String e = StringUtils.trim(r[1]);
            int sint = s.indexOf("[");
            int eint = e.indexOf("]");

            sint = sint < 0 ? 0 : sint + 1;
            eint = eint < 0 ? e.length() : eint;

            String start = StringUtils.trim(s.substring(sint));
            String end = StringUtils.trim(e.substring(0, eint));
            index.incrementAndGet();
            try {
                return new Long[]{ Long.valueOf(start), Long.valueOf(end) };
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("非法的数值区间：" + r[0] + " ，不能解析。");
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove()方法不被支持。");
        }

        @Override
        public Iterator<Long[]> iterator() {
            return this;
        }
    }


    public static class UnFormDoubleRange extends TimeRange<Double> {


        String rangeField;
        String[] dateRange;

        private AtomicInteger index = new AtomicInteger(0);

        public UnFormDoubleRange(String rangeField, String[] dateRange) {
            this.rangeField = rangeField;
            this.dateRange = dateRange;
        }


        @Override
        public boolean hasNext() {
            if(dateRange.length > index.get()) {
                return true;
            }
            return false;
        }

        @Override
        public Double[] next() {
            String range = dateRange[index.get()];

            String[] r = range.split("TO");
            String s = StringUtils.trim(r[0]);
            String e = StringUtils.trim(r[1]);
            int sint = s.indexOf("[");
            int eint = e.indexOf("]");

            sint = sint < 0 ? 0 : sint + 1;
            eint = eint < 0 ? e.length() : eint;

            String start = StringUtils.trim(s.substring(sint));
            String end = StringUtils.trim(e.substring(0, eint));
            index.incrementAndGet();
            try {
                return new Double[]{ Double.valueOf(start), Double.valueOf(end) };
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("非法的数值区间：" + r[0] + " ，不能解析。");
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove()方法不被支持。");
        }

        @Override
        public Iterator<Double[]> iterator() {
            return this;
        }
    }



    public static class UnFormFloatRange extends TimeRange<Float> {


        String rangeField;
        String[] dateRange;

        private AtomicInteger index = new AtomicInteger(0);

        public UnFormFloatRange(String rangeField, String[] dateRange) {
            this.rangeField = rangeField;
            this.dateRange = dateRange;
        }


        @Override
        public boolean hasNext() {
            if(dateRange.length > index.get()) {
                return true;
            }
            return false;
        }

        @Override
        public Float[] next() {
            String range = dateRange[index.get()];

            String[] r = range.split("TO");
            String s = StringUtils.trim(r[0]);
            String e = StringUtils.trim(r[1]);
            int sint = s.indexOf("[");
            int eint = e.indexOf("]");

            sint = sint < 0 ? 0 : sint + 1;
            eint = eint < 0 ? e.length() : eint;

            String start = StringUtils.trim(s.substring(sint));
            String end = StringUtils.trim(e.substring(0, eint));
            index.incrementAndGet();
            try {
                return new Float[]{ Float.valueOf(start), Float.valueOf(end) };
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("非法的数值区间：" + r[0] + " ，不能解析。");
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove()方法不被支持。");
        }

        @Override
        public Iterator<Float[]> iterator() {
            return this;
        }
    }
}
