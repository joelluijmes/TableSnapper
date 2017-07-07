namespace tableshot.Models
{
    public sealed class TableConfiguration
    {
        public ShallowTable Table { get; set; }
        
        public ReferencedByOptions ReferencedBy { get; set; }
    }
}
