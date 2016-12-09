using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Action_TinhLichThuHoachThang
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

    public class NgungTH
    {
        public DateTime tu;
        public DateTime den;
    }

    public class DoanTH
    {
        public Guid id;
        public DateTime tu;
        public DateTime den;
    }
}
