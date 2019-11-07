package cn.com.my.es;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.ExceptionUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


@Slf4j
public class ElasticSearchSinkUtil {


    public static void addSink(List<HttpHost> hosts, int bulkFlushMaxActions, int parallelism,
                               DataStream<Row> data, ElasticsearchSinkFunction<Row> func) {
        ElasticsearchSink.Builder<Row> esSinkBuilder = new ElasticsearchSink.Builder<>(hosts, func);
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        data.addSink(esSinkBuilder.build()).setParallelism(parallelism);
    }


    public static List<HttpHost> getEsAddresses(String hosts) throws MalformedURLException {
        String[] hostList = hosts.split(",");
        List<HttpHost> addresses = new ArrayList<>();
        for (String host : hostList) {
            if (host.startsWith("http")) {
                URL url = new URL(host);
                addresses.add(new HttpHost(url.getHost(), url.getPort()));
            } else {
                String[] parts = host.split(":", 2);
                if (parts.length > 1) {
                    addresses.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
                } else {
                    throw new MalformedURLException("invalid elasticsearch hosts format");
                }
            }
        }
        return addresses;
    }

    public static class RetryRejectedExecutionFailureHandler implements ActionRequestFailureHandler {
        private static final long serialVersionUID = -7423562912824511906L;

        public RetryRejectedExecutionFailureHandler() {
        }

        @Override
        public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
            if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
                indexer.add(action);
            } else {
                if (ExceptionUtils.findThrowable(failure, SocketTimeoutException.class).isPresent()) {
                    // 忽略写入超时，因为ElasticSearchSink 内部会重试请求，不需要抛出来去重启 flink job
                    return;
                } else {
                    Optional<IOException> exp = ExceptionUtils.findThrowable(failure, IOException.class);
                    if (exp.isPresent()) {
                        IOException ioExp = exp.get();
                        if (ioExp != null && ioExp.getMessage() != null && ioExp.getMessage().contains("max retry timeout")) {
                            // request retries exceeded max retry timeout, throw failure, restart flink job
                            log.error(ioExp.getMessage());
                            throw failure;
                        }
                    }
                }
                throw failure;
            }
        }
    }
}
