﻿using Microsoft.Xrm.Sdk;
using System;

namespace PbanBo_PhieuGiaoNhanHomGiong
{
    public class Tylethuhoivon
    {
        public EntityReference vuthuhoi;
        public decimal sotien;
        public decimal daphanbo;
        public Guid tylethuhoiid;
        public int namtaichinh;

        public Tylethuhoivon()
        {
            vuthuhoi = null;
            sotien = 0;
            daphanbo = 0;
            tylethuhoiid = Guid.Empty;
            namtaichinh = 0;
        }

        public Tylethuhoivon(EntityReference _namthuhoi, decimal _sotien, decimal _daphanbo, int _namtaichinh)
        {
            vuthuhoi = _namthuhoi;
            sotien = _sotien;
            daphanbo = _daphanbo;
            namtaichinh = _namtaichinh;
        }
    }
}