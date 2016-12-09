using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Action_GetPolygonData
{
    class GeoData
    {
        public string type;
        public GeoItem[] features;
    }

    class GeoItem
    {
        public string type;
        public GeoProperties properties;
        public Geometry geometry;
    }

    class GeoProperties
    {
        public string id;
        public bool a; //tuoi
        public int b; //loaisohuu
        public int c; //vutrong
        public int d; //loaigocmia
        public int e; //mucdichsanxuatmia
        public int f; //x day da trong
        public bool g; //miachay
        public decimal h; //dientich
        public string k; // khachhang
        public string l; //ma thua
        public string m; //nhomdat
        public string n; //giongmia - chin som
        public string o; //loai goc mia
        public string p; //loaisohuu
        public string q; //tram
        public string r; //co nguon nuoc
        public string color = "gray"; //default color
    }

    class Geometry
    {
        public string type;
        public decimal[][][] coordinates;
    }
}
