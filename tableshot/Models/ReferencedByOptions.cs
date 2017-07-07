using System;

namespace tableshot.Models
{
    [Flags]
    public enum ReferencedByOptions
    {
        Descending = 1,
        Ascending = 2,
        Schema = 4,
        Disabled = 0
    }
}
