using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kvstore
{
    class ProtocolCode
    {
        
	    public  const int Success = 0;

	    public  const int Invalid_Key = 1;

	    public  const int Dataserver_Io_Error = 2;

	    public  const int Invalid_Region = 3;

	    public  const int Master_Error = 4;

	    public  const int Duplicate_Dataserver_Error = 5;

	    public  const int Invalid_Counter = 6;

        private static IDictionary<int, String> messages;

        public static String getMessage(int code)
        {
            if (messages == null)
            {
                initMessage();
            }
             String value;
             messages.TryGetValue(code,out value);
             return value;
        }
        public static void initMessage()
        {
            messages = new Dictionary<int, String>();
            messages.Add(Success, "success");
            messages.Add(Invalid_Key, "the given key is not hosted in the data server");
            messages.Add(Dataserver_Io_Error, "fail to fetch data, data server internal error occurs");
            messages.Add(Invalid_Region, "the given region is not hosted in the data server");
            messages.Add(Master_Error, "error occured in master server");
            messages.Add(Duplicate_Dataserver_Error,
                    "the data server with same address already exists in cluster");
            messages.Add(Invalid_Counter, "the given key does not corespond to a counter");
        }
    }
}
