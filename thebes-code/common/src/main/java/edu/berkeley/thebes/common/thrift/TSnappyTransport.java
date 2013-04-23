/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package edu.berkeley.thebes.common.thrift;

import java.io.IOException;
import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.xerial.snappy.Snappy;

public class TSnappyTransport extends TTransport {

   /**
    * Underlying transport
    */
   private TTransport transport_ = null;
   /**
    * Buffer for output
    */
   private final TByteArrayOutputStream writeBuffer_ = new TByteArrayOutputStream(1024);
   private final TByteArrayOutputStream inputReadBuffer_ = new TByteArrayOutputStream();

   /**
    * Buffer for input
    */
   private TMemoryInputTransport readBuffer_ = new TMemoryInputTransport(new byte[0]);


   public static class Factory extends TTransportFactory {

       public Factory() {
       }

       @Override
       public TTransport getTransport(TTransport base) {
           return new TSnappyTransport(base);
       }
   }

   public TSnappyTransport(TTransport transport) {
       transport_ = transport;
   }

   public void open() throws TTransportException {
       transport_.open();
   }

   public boolean isOpen() {
       return transport_.isOpen();
   }

   public void close() {
       readBuffer_.reset(new byte[0]);
       writeBuffer_.reset();
       transport_.close();
   }

   public int read(byte[] buf, int off, int len) throws TTransportException {
       if(readBuffer_.getBytesRemainingInBuffer() == 0) {
           try {
               int bytesRead, pos = 0;
               byte[] tmpArray = new byte[1024];
               inputReadBuffer_.reset();

               while((bytesRead = transport_.read(tmpArray, pos, 1024)) != 0) {
                   inputReadBuffer_.write(tmpArray,  pos, bytesRead);
                   System.out.println("READ "+bytesRead);
                   pos += bytesRead;
               }

               readBuffer_.reset(Snappy.uncompress(inputReadBuffer_.toByteArray()));
           } catch (IOException e) {
               throw new TTransportException(e);
           }
       }

        return readBuffer_.read(buf, off, len);
   }

   @Override
   public byte[] getBuffer() {
       return readBuffer_.getBuffer();
   }

   @Override
   public int getBufferPosition() {
       return readBuffer_.getBufferPosition();
   }

   @Override
   public int getBytesRemainingInBuffer() {
       System.out.println("BYTESREMAINING "+readBuffer_.getBytesRemainingInBuffer());
       return readBuffer_.getBytesRemainingInBuffer();
   }

   @Override
   public void consumeBuffer(int len) {
       readBuffer_.consumeBuffer(len);
   }

   public void write(byte[] buf, int off, int len) throws TTransportException {
       writeBuffer_.write(buf, off, len);
   }

   @Override
   public void flush() throws TTransportException {
       try {
           transport_.write(Snappy.compress(writeBuffer_.toByteArray()));
           writeBuffer_.reset();
           transport_.flush();
       } catch (IOException ex) {
           throw new TTransportException(ex);
       }
   }
}