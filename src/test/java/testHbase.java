import com.sxbdjw.data.utils.hbase.HbaseUtils;

public class testHbase {
    public static void main(String[] args) {
        String tableName = "ods_mb_outer_thyh:test01";
        String rowkey = "20181111_88";
        String cf = "cf";
        String column = "click_count";
        String value = "2";

        HbaseUtils.getInstance().put(tableName,rowkey,cf,column,value);
    }
}
