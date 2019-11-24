package cn.com.my.spark.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;

public class MyHexRegionSplit extends RegionSplitter.HexStringSplit{

    final static String DEFAULT_MIN_HEX = "00000000";
    final static String DEFAULT_MAX_HEX = "FFFFFFFF";

    public MyHexRegionSplit () {
        super();
    }

    @Override
    public byte[][] split(byte[] start, byte[] end, int numSplits, boolean inclusive) {

        if(StringUtils.isEmpty(Bytes.toString(start))){
            start = Bytes.toBytes(DEFAULT_MIN_HEX);
        }

        if(StringUtils.isEmpty(Bytes.toString(end))){
            end = Bytes.toBytes(DEFAULT_MAX_HEX);
        }
        return super.split(start, end, numSplits, inclusive);
    }
}
