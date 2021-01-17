package com.kinesis.producer.test.kinesisdatastreamproducer;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import software.amazon.kinesis.common.KinesisClientUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class sampleProducer {

    static private String STREAM_NAME = "milk-data-stream";
    public static void main(String args[]) {

        BasicAWSCredentials awsCreds = new BasicAWSCredentials("aws_access_key_id",
                "aws_secret_access_key");
        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
        clientBuilder.setRegion("us-east-1"); // region
        clientBuilder.setCredentials(new AWSStaticCredentialsProvider(awsCreds));
        AmazonKinesis kinesisClient = clientBuilder.build();


          



        while(true)
        {
            try{
                for (int i = 0; i < 100; i++) {
                    sendData(kinesisClient, i);
                    sendData2(i);
                }   
                Thread.sleep(5000);

            }catch(Exception e)
            {
                System.out.println(e.toString());
            }   
        }


        // 100번 반복해서 전송
      
    }

    static void sendData2(int count) {

        
        String myData = "{\"no\":" + count + "}\n"; // 보내려는 데이터
        Region region = Region.of("us-east-1");
        for(int i=0; i<10; i++)
        {
            PutRecordRequest request = PutRecordRequest.builder()
                .partitionKey(String.format("partitionKey-%d", i))
                .streamName(STREAM_NAME)
                .data(SdkBytes.fromByteArray(myData.getBytes()))
                .build();               
            
            try {

                KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(region));

                kinesisClient.putRecord(request).get();
            } catch (Exception e) {
                System.out.println(e.toString());
            
            }          
        }
        
       
    }

    static void sendData(AmazonKinesis kinesisClient, int count) {
        String myData = "{\"no\":" + count + "}\n"; // 보내려는 데이터
        PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
        putRecordsRequest.setStreamName(STREAM_NAME);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(ByteBuffer.wrap(myData.getBytes()));
            putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", i));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }
        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        PutRecordsResult putRecordsResult  = kinesisClient.putRecords(putRecordsRequest);
        System.out.println("Put Result" + putRecordsResult);
    }
}