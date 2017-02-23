using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace Action_TinhPDNThuNo
{
    public class Phanbodautu
    {
        public Guid ID { get; set; }
        public decimal sotien { get; set; }
        public decimal laisuat { get; set; }
        public int loailaisuat { get; set; }
        public EntityReference thuacanhtac { get; set; }
        public EntityReference hopdongmia { get; set; }
        public DateTime ngayphatsinh { get; set; }
        public DateTime ngaytinhlaisaucung { get; set; }
        public decimal conlai { get; set; }
        public decimal nolai { get; set; }
        public string masohphieuphanbo { get; set; }
        public int namtaichinhvuthanhtoan { get; set; }
        public int loaihopdong { get; set; }
        public int status{ get; set; }

        public EntityReference vudautu { get; set; }

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
        private bool _isBefore = false;
        public bool isOver
        {
            get { return _isOver; }
            set { _isOver = value; }
        }
        public bool isBefore
        {
            get { return _isBefore; }
            set { _isBefore = value; }
        }
    }

    class phieutinhlai
    {
        public int sn { get; set; }
        public decimal sotien { get; set; }
        public decimal ls { get; set; }
        public DateTime fr { get; set; }
        public DateTime to { get; set; }
        public decimal tl { get; set; }

        public int status { get; set; }

        public phieutinhlai(int _sn, decimal _sotien, decimal _ls, DateTime _fr, DateTime _to, decimal _tl,int _status)
        {
            sn = _sn;
            sotien = _sotien;
            ls = _ls;
            fr = _fr;
            to = _to;
            tl = _tl;
            status = _status;
        }
    }
}
