using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Plugin_AutoSum
{
    class Field
    {
        public string parentfield;
        public string childfield;
        public string agg;
        public string datatype;

        public Field(string parentfield, string childfield, int agg, string datatype)
        {
            this.parentfield = parentfield;
            this.childfield = childfield;
            this.agg = getAggre(agg);
            this.datatype = datatype;
        }

        string getAggre(int agg)
        {
            switch (agg)
            {
                case 100000000:
                    return "sum";
                case 100000001:
                    return "avg";
                case 100000002:
                    return "min";
                case 100000003:
                    return "max";
                default:
                    return "count";
            }
        }
    }

    class NhomLookup
    {
        public string parentEntity;
        public string lookupField;
        public List<Field> fields = new List<Field>();

        public NhomLookup(string parententity, string lookupfield, string childfield, string parentfield, int agg, string datatype)
        {
            this.parentEntity = parententity;
            this.lookupField = lookupfield;
            this.fields.Add(new Field(parentfield, childfield, agg, datatype));
        }

        public string GetParentField(string childfield)
        {
            foreach (Field a in this.fields)
            {
                if (a.childfield == childfield)
                    return a.parentfield;
            }
            return "";
        }
    }

    class NhomLookupCollection
    {
        public List<NhomLookup> NhomLookups = new List<NhomLookup>();

        public bool CheckLookup(string lookupField)
        {
            foreach (NhomLookup a in NhomLookups)
                if (a.lookupField == lookupField)
                    return true;
            return false;
        }

        public NhomLookup GetNhomLookup(string lookupField)
        {
            foreach (NhomLookup a in NhomLookups)
                if (a.lookupField == lookupField)
                    return a;
            return null;
        }

        public void AddRecord(string parententity, string lookupfield, string childfield, string parentfield, int agg, string datatype)
        {
            NhomLookup tmp = this.GetNhomLookup(parententity);
            if (tmp == null)
                this.NhomLookups.Add(new NhomLookup(parententity, lookupfield, childfield, parentfield, agg, datatype));
            else
            {
                tmp.fields.Add(new Field(parentfield, childfield, agg, datatype));
            }
        }
    }
}
