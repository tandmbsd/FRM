namespace PbanBo_PhieuGiaoNhanHomGiong
{
    public class DinhMuc
    {
        public decimal dinhMucHL;
        public decimal dinhMucKHL;

        public static decimal tongdinhmucHL = 0;
        public static decimal tongdinhmucKHL = 0;

        public DinhMuc(decimal _dinhMucHL, decimal _dinhMucKHL)
        {
            this.dinhMucHL = _dinhMucHL;
            this.dinhMucKHL = _dinhMucKHL;
            tongdinhmucHL += _dinhMucHL;
            tongdinhmucKHL += _dinhMucKHL;
        }
    }
}