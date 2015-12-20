package com.ivyft.katta.protocol;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/4/8
 * Time: 17:00
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class IntLengthHeaderFile {




    private IntLengthHeaderFile() {

    }

    public static class Reader implements Iterator<byte[]> {

        protected final DataInputStream in;


        protected int nextLength = -1;



        public Reader(FileSystem fs, Path path) {
            this(fs.getConf(), fs, path, fs.getConf().getInt("io.file.buffer.size", 4096));
        }


        public Reader(Configuration conf, FileSystem fs, Path path) {
            this(conf, fs, path, fs.getConf().getInt("io.file.buffer.size", 4096));
        }


        public Reader(FileSystem fs, Path path, int buffer) {
            this(fs.getConf(), fs, path, fs.getConf().getInt("io.file.buffer.size", 4096));
        }

        public Reader(Configuration conf, FileSystem fs, Path path, int buffer) {
            try {
                this.in = fs.open(path, buffer);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }


        public synchronized void close() throws IOException {
            in.close();
        }




        public int available(){
            try {
                return in.available();
            } catch (IOException e) {
                return -1;
            }
        }



        @Override
        public synchronized boolean hasNext() {
            try {
                if(in.available() > 0 ) {
                    this.nextLength = this.in.readInt();
                    if(in.available() == 0) {
                        return false;
                    }
                    return this.nextLength > 0;
                }
                return false;
            } catch (EOFException e) {
                return false;
            } catch (IOException e) {
                return false;
            }
        }

        @Override
        public synchronized byte[] next() {
            byte[] buffer = new byte[this.nextLength];
            try {
                in.readFully(buffer);
            } catch (IOException e) {
                return new byte[0];
            }
            return buffer;
        }



        public synchronized BytesWritable nextBytesWritable() {
            byte[] buffer = new byte[this.nextLength];
            try {
                in.readFully(buffer);
            } catch (IOException e) {
                return new BytesWritable();
            }
            return new BytesWritable(buffer);
        }




        public synchronized ByteBuffer nextByteBuffer() {
            byte[] buffer = new byte[this.nextLength];
            try {
                in.readFully(buffer);
            } catch (IOException e) {
                return ByteBuffer.allocate(0);
            }
            return ByteBuffer.wrap(buffer);
        }



        @Override
        public void remove() {
            throw new UnsupportedOperationException("unsupported remote operation.");
        }
    }



    public static class Writer {


        protected final DataOutputStream out;


        public Writer(FileSystem fs, Path path) {
            this(fs, path, true);
        }


        public Writer(FileSystem fs, Path path, boolean overwrite) {
            try {
                this.out = fs.create(path, overwrite);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }


        public synchronized void write(BytesWritable message) throws IOException {
            write(message.getBytes());
        }


        public synchronized void write(byte[] message) throws IOException {
            out.writeInt(message.length);
            out.write(message);
        }


        public synchronized void write(ByteBuffer message) throws IOException {
            if(message.limit() == message.capacity() && message.position() > 0) {
                message.flip();
            }
//            System.out.println("capacity: " + message.capacity() + " limit:" +
//                    message.limit() + " position: " + message.position());
            out.writeInt(message.limit());
            out.write(message.array());
        }


        public synchronized void flush() throws IOException {
            out.flush();
        }

        public synchronized void close() throws IOException {
            out.flush();
            out.close();
        }
    }
}
