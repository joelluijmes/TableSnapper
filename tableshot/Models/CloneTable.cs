namespace tableshot.Models
{
    public sealed class CloneTable
    {
        public ShallowTable Table { get; set; }
        
        public ReferencedByOptions ReferencedBy { get; set; }
    }
}
