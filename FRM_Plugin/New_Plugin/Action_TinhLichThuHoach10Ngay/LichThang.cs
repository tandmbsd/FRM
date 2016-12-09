using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Workflow_TinhLichThuHoach10Ngay
{

    public class TramTH
    {
        public Guid Tram;
        public int Thang11 = 0;
        public int Thang12 = 0;
        public int Thang1 = 0;
        public int Thang2 = 0;
        public int Thang3 = 0;
        public int Thang4 = 0;
        public Dictionary<Guid, int> quotadot = new Dictionary<Guid, int>();
        public List<Thuadat> dsthua = new List<Thuadat>();
    }

    public class DotTH
    {
        public Guid id;
        public DateTime tu;
        public DateTime den;
        public Dictionary<int, int> phanbo = new Dictionary<int, int>();
    }

    public class DoanTH
    {
        public Guid id;
        public DateTime tu;
        public DateTime den;
        public int tgth1thua = 0;
        public decimal quotathua = 0;
        public decimal quotakh = 0;
        public int kltoithieu1thua = 0;
        public List<Thutuuutien> ttuutien = new List<Thutuuutien>();
        public Dictionary<Guid, int> tltoithieu = new Dictionary<Guid, int>();
        public Dictionary<Guid, DotTH> dotth = new Dictionary<Guid, DotTH>();
        public Dictionary<Guid, int> thutuuutienTHSom = new Dictionary<Guid, int>(); //<STT, Lydo>
    }

    public class Thuadat
    {
        public Guid ThuaId;
        public Guid KhachHangId;
        public DateTime NgayTHDK;
        public int Loaigocmia = 100000000;
        public int Luugoc = -1;
        public DateTime NgayKTBP = new DateTime(3000, 1, 1);
        public bool Tuoi;
        public decimal NoQuaHan = 0;
        public decimal Sanluong = 0;
        public Guid Lydothsom = Guid.Empty;
        //public Guid CTBBThuhoachsom = Guid.Empty;
        public decimal Tuoimia = 0;
        public DateTime Ngaytrong = new DateTime(1900, 1, 1);
        public int flag = 1;
        public int orderTHS = 2147483647;
    }

    public class Thutuuutien
    {
        public int stt;
        public int lydo;

        public Thutuuutien(int stt, int lydo)
        {
            this.stt = stt;
            this.lydo = lydo;
        }
    }
}
