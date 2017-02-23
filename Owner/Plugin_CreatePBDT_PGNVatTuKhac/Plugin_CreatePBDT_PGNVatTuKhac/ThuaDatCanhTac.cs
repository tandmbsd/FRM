using Microsoft.Xrm.Sdk;
using System;
using System.Collections.Generic;

namespace PbanBo_PhieuGiaoNhanHomGiong
{
    public class ThuaDatCanhTac
    {
        public decimal DinhMucHL;
        public decimal DinhMucKHL;
        public decimal TongDMHL;
        public decimal TongDMKHL;
        public Entity thuadatcanhtac;
        private Guid id;

        public static decimal sumhl = 0;
        public static decimal sumkhl = 0;

        public ThuaDatCanhTac(decimal _DinhMucHL, decimal _DinhMucKHL, Entity _thuadatcanhtac)
        {
            DinhMucHL = _DinhMucHL;
            DinhMucKHL = _DinhMucKHL;
            sumhl += DinhMucHL;
            sumkhl += DinhMucKHL;
            thuadatcanhtac = _thuadatcanhtac;
        }

        public ThuaDatCanhTac()
        {
            id = Guid.Empty;
            DinhMucHL = 0;
            DinhMucKHL = 0;
        }
    }

    internal class ThuaDatCanhTacColection
    {
        public List<ThuaDatCanhTac> ListThuaDatCanhtac = new List<ThuaDatCanhTac>();

        public void AddRecord(ThuaDatCanhTac a)
        {
        }
    }
}