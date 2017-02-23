using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace TinhNo_Phieutinhtienmia
{
    public class Phanbodautu
    {
        public Guid ID { get; set; }
        public decimal sotien { get; set; }
        public decimal laisuat { get; set; }
        public int loailaisuat { get; set; }
        public EntityReference thuacanhtac { get; set; }
        public DateTime ngaytinhlaisaucung { get; set; }
        public DateTime ngayphatsinh { get; set; }
        public decimal conlai { get; set; }
        public decimal nolai { get; set; }

        public Phanbodautu()
        {
            sotien = 0;
            laisuat = 0;
            loailaisuat = 0;
            ngaytinhlaisaucung = new DateTime();
        }
    }
    class point
    {
        public int index { get; set; }
        public int sn { get; set; }
        private bool _isOver = false;
        public bool isOver
        {
            get { return _isOver; }
            set { _isOver = value; }
        }
    }
}
