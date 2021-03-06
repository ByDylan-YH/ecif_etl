package entity.hbase;

import annotation.EnumStoreType;
import annotation.HBaseColumn;
import annotation.HBaseTable;
import lombok.Data;

import java.io.Serializable;

/**
 * Author:BYDylan
 * Date:2020/9/9
 * Description:
 */
//@Data
@HBaseTable(tableName = ":PTY_CUST_FLG")
public class PTY_CUST_FLGEntity implements Serializable {
    @HBaseColumn(qualifier = "pbk_id", columnType = EnumStoreType.EST_STRING)
    private String rowkey;

    @HBaseColumn(qualifier = "bk_id", columnType = EnumStoreType.EST_STRING)
    private String bk_id;

    @HBaseColumn(qualifier = "row_lock", columnType = EnumStoreType.EST_STRING)
    private String row_lock;

    @HBaseColumn(qualifier = "cust_id", columnType = EnumStoreType.EST_STRING)
    private String cust_id;

    @HBaseColumn(qualifier = "name", columnType = EnumStoreType.EST_STRING)
    private String name;

    @HBaseColumn(qualifier = "cert_type_cd", columnType = EnumStoreType.EST_STRING)
    private String cert_type_cd;

    @HBaseColumn(qualifier = "cert_no", columnType = EnumStoreType.EST_STRING)
    private String cert_no;

    @HBaseColumn(qualifier = "cust_type_cd", columnType = EnumStoreType.EST_STRING)
    private String cust_type_cd;

    @HBaseColumn(qualifier = "src_sys", columnType = EnumStoreType.EST_STRING)
    private String src_sys;

    @HBaseColumn(qualifier = "upd_sys", columnType = EnumStoreType.EST_STRING)
    private String upd_sys;

    @HBaseColumn(qualifier = "data_crt_tm", columnType = EnumStoreType.EST_STRING)
    private String data_crt_tm;

    @HBaseColumn(qualifier = "data_upd_tm", columnType = EnumStoreType.EST_STRING)
    private String data_upd_tm;

    @HBaseColumn(qualifier = "etl_crt_tm", columnType = EnumStoreType.EST_STRING)
    private String etl_crt_tm;

    @HBaseColumn(qualifier = "etl_upd_tm", columnType = EnumStoreType.EST_STRING)
    private String etl_upd_tm;

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public String getBk_id() {
        return bk_id;
    }

    public void setBk_id(String bk_id) {
        this.bk_id = bk_id;
    }

    public String getRow_lock() {
        return row_lock;
    }

    public void setRow_lock(String row_lock) {
        this.row_lock = row_lock;
    }

    public String getCust_id() {
        return cust_id;
    }

    public void setCust_id(String cust_id) {
        this.cust_id = cust_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCert_type_cd() {
        return cert_type_cd;
    }

    public void setCert_type_cd(String cert_type_cd) {
        this.cert_type_cd = cert_type_cd;
    }

    public String getCert_no() {
        return cert_no;
    }

    public void setCert_no(String cert_no) {
        this.cert_no = cert_no;
    }

    public String getCust_type_cd() {
        return cust_type_cd;
    }

    public void setCust_type_cd(String cust_type_cd) {
        this.cust_type_cd = cust_type_cd;
    }

    public String getSrc_sys() {
        return src_sys;
    }

    public void setSrc_sys(String src_sys) {
        this.src_sys = src_sys;
    }

    public String getUpd_sys() {
        return upd_sys;
    }

    public void setUpd_sys(String upd_sys) {
        this.upd_sys = upd_sys;
    }

    public String getData_crt_tm() {
        return data_crt_tm;
    }

    public void setData_crt_tm(String data_crt_tm) {
        this.data_crt_tm = data_crt_tm;
    }

    public String getData_upd_tm() {
        return data_upd_tm;
    }

    public void setData_upd_tm(String data_upd_tm) {
        this.data_upd_tm = data_upd_tm;
    }

    public String getEtl_crt_tm() {
        return etl_crt_tm;
    }

    public void setEtl_crt_tm(String etl_crt_tm) {
        this.etl_crt_tm = etl_crt_tm;
    }

    public String getEtl_upd_tm() {
        return etl_upd_tm;
    }

    public void setEtl_upd_tm(String etl_upd_tm) {
        this.etl_upd_tm = etl_upd_tm;
    }
}
