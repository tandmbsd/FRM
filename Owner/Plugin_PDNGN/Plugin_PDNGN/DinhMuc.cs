namespace Plugin_PDNGN
{
    public class DinhMuc
    {
        public decimal dinhMucHL;
        public decimal dinhMucKHL;
        public string name;

        public static decimal tongdinhmucHL = 0;
        public static decimal tongdinhmucKHL = 0;

        public DinhMuc(decimal _dinhMucHL, decimal _dinhMucKHL, string _name)
        {
            this.dinhMucHL = _dinhMucHL;
            this.dinhMucKHL = _dinhMucKHL;
            tongdinhmucHL += _dinhMucHL;
            tongdinhmucKHL += _dinhMucKHL;
            this.name = _name;
        }
    }
}
