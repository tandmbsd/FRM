using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.Discovery;
using Microsoft.Xrm.Client.Services;
using Microsoft.Xrm.Client;

namespace HDThueDatConsoleApp
{
    public class ServerConnection
    {
        private string _connectionString;
        private CrmConnection _crmConnection;
        public CrmConnection CRMConnection { get { return _crmConnection; } }
        public ServerConnection(string connectionstring)
        {
            _connectionString = connectionstring;
            _crmConnection = CrmConnection.Parse(_connectionString);
        }
    }
}
