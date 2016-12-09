using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Action_TinhLichThuHoachChiTiet
{
    public class LichThang
    {
        public Guid Tram;
        public int Thang11 = 0;
        public int Thang12 = 0;
        public int Thang1 = 0;
        public int Thang2 = 0;
        public int Thang3 = 0;
        public int Thang4 = 0;
    }

    public class DotTH
    {
        public Guid dot;
        public DateTime tu;
        public DateTime den;
        public int songay;
        public Dictionary<int, int> phanbo = new Dictionary<int, int>();
    }

    public class Thuadat
    {
        public Guid ThuaId;
        public Guid KhachHangId;
        public DateTime NgayTHDK;
        public decimal Sanluong;
    }

}
