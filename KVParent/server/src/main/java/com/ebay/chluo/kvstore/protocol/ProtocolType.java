package com.ebay.chluo.kvstore.protocol;

public interface ProtocolType {
	public static final int Heart_Beart_Req = 0;
	public static final int Heart_Beart_Resp = 1;

	public static final int Load_Region_Req = 2;
	public static final int Load_Region_Resp = 3;

	public static final int Unload_Region_Req = 4;
	public static final int Unload_Region_Resp = 5;

	public static final int Split_Region_Req = 6;
	public static final int Split_Region_Resp = 7;

	public static final int Set_Req = 10001;
	public static final int Set_Resp = 10002;

	public static final int Get_Req = 10003;
	public static final int Get_Resp = 10004;

	public static final int Delete_Req = 10005;
	public static final int Delete_Resp = 10006;

	public static final int Incr_Req = 10007;
	public static final int Incr_Resp = 10008;

	public static final int Stat_Req = 10009;
	public static final int Stat_Resp = 10010;

	public static final int Region_Table_Req = 10011;
	public static final int Region_Table_Resp = 10012;
}
