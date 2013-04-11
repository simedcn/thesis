using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kvstore
{
    class KVProtocolType
    {

        public  const int Set_Req = 10001;
	    public  const int Set_Resp = 10002;

	    public  const int Get_Req = 10003;
	    public  const int Get_Resp = 10004;

	    public  const int Delete_Req = 10005;
	    public  const int Delete_Resp = 10006;

	    public  const int Incr_Req = 10007;
	    public  const int Incr_Resp = 10008;
	    public  const int Stat_Req = 10009;
	    public  const int Stat_Resp = 10010;
	    public  const int Region_Table_Req = 10011;
	    public  const int Region_Table_Resp = 10012;
    }
}
