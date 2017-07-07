namespace tableshot.Models
{
    public sealed class TableConfiguration
    {
        public ShallowTable Table { get; set; }

        public ReferencedByOptions ReferencedBy { get; set; }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;

            return obj is TableConfiguration && Equals((TableConfiguration) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((int) ReferencedBy * 397) ^ (Table != null ? Table.GetHashCode() : 0);
            }
        }

        public static bool operator ==(TableConfiguration left, TableConfiguration right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(TableConfiguration left, TableConfiguration right)
        {
            return !Equals(left, right);
        }

        public override string ToString()
        {
            return $"{Table}:{ReferencedBy}";
        }

        private bool Equals(TableConfiguration other)
        {
            return ReferencedBy == other.ReferencedBy && Equals(Table, other.Table);
        }
    }
}
