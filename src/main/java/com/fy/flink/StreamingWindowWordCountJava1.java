package com.fy.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class StreamingWindowWordCountJava1 {

    public static void main(String[] args) throws Exception {

        //定义socket的端口号
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e) {
            System.out.println("没有指定port参数，是用默认值9000");
            port = 9000;
        }

        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //连接socket获取输入的数据
//        DataStreamSource<String> text = env.socketTextStream("localhost", port, "\n");

        String filePath = "C:\\Users\\fuyu15\\Desktop\\test.txt";
        String filePathOut = "C:\\Users\\fuyu15\\Desktop\\test_out.txt";
        DataStream<String> text = env.readTextFile(filePath);

        DataStream<Map<String, Object>> windowCount = text
                .map(new MapFunction<String, Map<String,Object>>() {
                    @Override
                    public Map<String,Object> map(String value) throws Exception {
                        System.out.println("value-------------" + value);
                        String[] splits = value.split("\\s");
                        Map<String,Object> tempMap = new HashMap<String,Object>();
                        int i = 1;
                        for (String word:splits) {
                            tempMap.put(("key" + i).toString(),word);
                            i++;
//                            out.collect(new StreamingWindowWordCountJava.WordWithCount(word,1L));
                        }
                        return tempMap;
                    }
                })//打平操作，把每行的单词转为Map<String,Object>类型的数据
                .filter(new FilterFunction<Map<String, Object>>() {
                    @Override
                    public boolean filter(Map<String, Object> map) throws Exception {
                        if (map != null) {
                            Object typValue = map.get("key1");
                            if (typValue != null) {
//                                System.out.println("typeValue ============" + typValue.toString());
                                for (int j = 1; j <= map.size(); j++){
                                    String res = map.get("key" + j).toString() + ",";
                                    System.out.println(res);
                                }
                                return true;
                            }
                        }
                        return false;
                    }
                });

        //将内容写回文件
//        new FileOutputFormat<>()
//        new FileOutputFormat(filePathOut).configure(filePathOut);

        windowCount.print().setParallelism(1);

        //注意，因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("streaming word count");

    }

    /**
     * 主要为了存储单词以及单词出现的次数
     */
    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount(){}
        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}
