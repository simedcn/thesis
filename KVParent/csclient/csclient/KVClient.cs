using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.Net;

namespace kvstore
{
    class KVClient : IKVClient
    {
        protected IDictionary<Address, Socket> connections;

        protected RegionTable table;

        protected ClientOption option;

        protected Address activeMaster;

        public KVClient(ClientOption option)
        {
            this.option = option;
            this.connections = new Dictionary<Address, Socket>();
            activeMaster = null;
        }

        public ClientOption GetClientOption()
        {
            return option;
        }

        public void Close()
        {
            ICollection<Socket> sockets = connections.Values;
            foreach (Socket socket in sockets)
            {
                socket.Close();
            }
            connections.Clear();
        }

        protected void checkKey(byte[] key)
        {
            if (key == null)
            {
                throw new System.NullReferenceException("null key is not allowed");
            }
        }

        protected Socket getConnection(Address addr)
        {
            Socket socket = null;
            connections.TryGetValue(addr, out socket);
            if (socket == null || !socket.Connected)
            {
                socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                socket.Connect(new IPEndPoint(IPAddress.Parse(addr.Ip), addr.Port));
                connections.Add(addr, socket);
            }
            return socket;
        }


        protected Socket getConnection(byte[] key)
        {
            if (table == null)
            {
                UpdateRegionTable();
            }
            Address addr = table.getKeyAddr(key);
            if (addr == null)
            {
                // fail to get key for region.
                UpdateRegionTable();
                addr = table.getKeyAddr(key);
            }
            if (addr == null)
            {
                throw new KVException("Fail to get region for key:" + key.ToString());
            }
            Socket socket = null;
            try
            {
                socket = getConnection(addr);
            }
            catch (Exception e)
            {
                System.Console.Error.WriteLine("Fail to connect to " + addr);
                UpdateRegionTable();
                addr = table.getKeyAddr(key);
                socket = getConnection(addr);
            }
            return socket;
        }

        protected Socket getMasterConnection()
        {
            Socket socket = null;
            if (activeMaster != null)
            {
                try
                {
                    socket = getConnection(activeMaster);
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine("Fail to connect to master " + activeMaster);
                }
            }
            if (socket == null)
            {
                ICollection<Address> masters = option.MasterAddrs;
                foreach (Address master in masters)
                {
                    try
                    {
                        socket = getConnection(master);
                        if (socket != null)
                        {
                            activeMaster = master;
                            return socket;
                        }
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine("Fail to connect to master " + activeMaster + " try next");
                    }
                }
            }
            if (socket != null)
            {
                return socket;
            }
            else
            {
                throw new KVException(
                        "No active master found in cluster, please check the configuration.");
            }
        }


        public void UpdateRegionTable()
        {
            Socket socket = getMasterConnection();

            SocketUtil.WriteUpdateRegionTableRequest(socket);
            RegionTable newTable = null;
            int ret = SocketUtil.ReadRegionTableResponse(socket, out newTable);
            if (ret != ProtocolCode.Success)
            {
                throw new KVException(ProtocolCode.getMessage(ret));
            }
            this.table = newTable;
        }

        public void Delete(byte[] key)
        {
            checkKey(key);
            delete(key, true);
        }

        private void delete(byte[] key, bool retry)
        {
            Socket socket = getConnection(key);
            SocketUtil.WriteDeleteRequest(socket, key);
            int ret = SocketUtil.ReadDeleteResponse(socket);
            if (ret == ProtocolCode.Invalid_Key && retry)
            {
                UpdateRegionTable();
                delete(key, false);
                return;
            }
            else if (ret != ProtocolCode.Success)
            {
                throw new KVException(ProtocolCode.getMessage(ret));
            }

        }

        public byte[] Get(byte[] key)
        {
            checkKey(key);
            return get(key, true);
        }

        private byte[] get(byte[] key, bool retry)
        {
            Socket socket = getConnection(key);
            SocketUtil.WriteGetRequest(socket, key);
            byte[] value = null;
            int ret = SocketUtil.ReadGetResponse(socket, out value);
            if (ret == ProtocolCode.Invalid_Key && retry)
            {
                UpdateRegionTable();
                return get(key, false);
            }
            else if (ret != ProtocolCode.Success)
            {
                throw new KVException(ProtocolCode.getMessage(ret));
            }
            else
            {
                return value;
            }
        }

        public void Set(byte[] key, byte[] value)
        {
            checkKey(key);
            if (value == null)
            {
                throw new NullReferenceException("null value is not allowed");
            }
            set(key, value, true);
        }

        private void set(byte[] key, byte[] value, bool retry)
        {
            Socket socket = getConnection(key);
            SocketUtil.WriteSetRequest(socket, key, value);
            int ret = SocketUtil.ReadSetResponse(socket);
            if (ret == ProtocolCode.Invalid_Key && retry)
            {
                UpdateRegionTable();
                set(key, value, false);
            }
            else if (ret != ProtocolCode.Success)
            {
                throw new KVException(ProtocolCode.getMessage(ret));
            }
        }

        public int GetCounter(byte[] key)
        {
            byte[] value = Get(key);
            if (value == null || value.Length != 4)
            {
                throw new KVException("The key:" + key.ToString() + " is not a valid counter");
            }
            return KeyValueUtil.bytesToInt(value);
        }


        public int Incr(byte[] key, int incremental, int initValue)
        {
            checkKey(key);
            return incr(key, incremental, initValue, true);
        }
        private int incr(byte[] key, int incremental, int initValue, bool retry)
        {
            Socket socket = getConnection(key);
            SocketUtil.WriteIncrRequest(socket, key, incremental, initValue);
            int value = 0;
            int ret = SocketUtil.ReadIncrResponse(socket, out value);
            if (ret == ProtocolCode.Invalid_Key && retry)
            {
                UpdateRegionTable();
                return incr(key, incremental, initValue, false);
            }
            else if (ret != ProtocolCode.Success)
            {
                throw new KVException(ProtocolCode.getMessage(ret));
            }
            else
            {
                return value;
            }
        }

        public DataServerStruct[] Stat()
        {
            return stat(true);
        }

        private DataServerStruct[] stat(bool retry)
        {
            Socket socket = getMasterConnection();
            SocketUtil.WriteStatRequest(socket);
            DataServerStruct[] dataServers = null;
            int ret = SocketUtil.ReadStatResponse(socket, out dataServers);
            if (ret == ProtocolCode.Invalid_Key && retry)
            {
                UpdateRegionTable();
                return stat(false);
            }
            else if (ret != ProtocolCode.Success)
            {
                throw new KVException(ProtocolCode.getMessage(ret));
            }
            else
            {
                return dataServers;
            }

        }
    }
}
