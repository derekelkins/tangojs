# Untangling the Tango paper

## What

The [Tango paper](http://www.cs.cornell.edu/~taozou/sosp13/tangososp.pdf) looks interesting, but it isn't the
clearest paper. It's mainly lacking detail and clear definitions. This is my attempt to guess at some of the
details. I'm less interested in the CORFU parts, so I will spend less time on them.

## Sections

### Introduction

This part is reasonably clear for an introduction. The only slightly ambiguous aspect is when they say "client",
they mean client to the shared log, which will usually be some sort of server as in the TangoZK example.

### Background

This section is mostly about CORFU and much of it is irrelevant. The naming of two of the CORFU operations is
misleading. The `check` operation simply returns the latest log index. The `trim` operations marks a specific
log entry for deletion. Once marked, the entry is unreadable. Only that entry is affected.

### Tango Architecture

This section leaves a lot of details unspecified, implied, or gives only a sentence or two of elaboration.

The Tango runtime is under-emphasized in the presentation. In addition to providing the `update_helper` and
`query_helper` methods and up-calling `apply`, the runtime also maintains a mapping from human friendly names
to `oid`s. Included in the mapping is the offset of the latest `forget` for each object. That `update_helper`
and `query_helper` actually provide a more involved interface: we want to have a key so we can have only parts
of an object update (e.g. different keys in a hash map) and we want to support restoring to historical snapshots.

*TODO* Explicate example in Durability subsection.

```csharp
// Most of the methods that interact with the log would likely be presented async in actuality.

interface ICorfu {
    int Append(LogEntry entry);
    void Trim(int offset);
    LogEntry Read(int offset);
    int TailOffset(); // "Check"
}

interface ITangoObject {
    Unique Oid { get; }
    void Apply(Opaque value, int offset, Opaque? key);

    // They don't mention needing an up-call for this, but I'm not sure how else to do it.
    // There are alternative approaches that I can think of, but they seem more horrible than this.
    void ApplyCheckpoint(Opaque state, int offset);
    bool Conflicts(Opaque oldValue, Opaque newValue);
}

interface ITangoDirectory : ITangoObject {
    Entry getByOid(Unique oid);
    Entry getByName(string name);
    void put(Entry e); // or perhaps something more fine-grained
}

struct Entry {
    Unique Oid;
    string Name;
    int forgetOffset;
    int latestCheckpoint; // ??? I'm guessing this is tracked here.  This is safe to read stale in QueryHelper.
}

interface ITangoRuntime { 
    void UpdateHelper(Unique oid, Opaque value, Opaque? key); 
    void QueryHelper(Unique oid, int? stopOffset);
}

interface ITangoTransaction : ITangoRuntime {
    bool Commit(); // Both Commit and Abort can fail, though it would be surprising if Abort failed.
    bool Abort();
}

class TangoRuntime : ITangoRuntime, ITangoDirectory {
    void Checkpoint(Unique oid, Opaque state);
    void Forget(int offset);
    ITangoTransaction BeginTransaction();
}
```
```csharp
// Register example as I would do it (not relying on thread-local state).
class TangoRegister<T> : ITangoObject where T : Opaque {
    private readonly Unique oid;
    private T value;

    public TangoRegister(Unique oid, T initialValue) {
        this.oid = oid; this.value = initialValue;
    }

    public Unique Oid { get { return oid; } }
    
    internal void Apply(Opaque value, int offset, Opaque? key) {
        this.value = (T)value;
    }

    internal void ApplyCheckpoint(Opaque state, int offset) {
        this.value = (T)state;
    }

    public void Checkpoint(TangoRuntime rt) {
        rt.Checkpoint(this.Oid, this.value);
    }

    public T Get(ITangoRuntime rt) {
        rt.QueryHelper(this.Oid);
        return this.value;
    }

    public void Set(ITangoRuntime rt) {
        rt.UpdateHelper(this.Oid, this.value, null);
    }
}
```

```csharp
// Guess at (high-level) on-disk structure. (Ignoring "filling" as that's a detail of CORFU, not really Tango.)
union LogEntry {
    Update, CommitRecord, DecisionRecord, Checkpoint 
}

struct Update {
    Unique Oid;
    Opaque? Key; // Really this would be in the Value.  As the conflict mechanism is pluggable.
    Opaque Value;
    bool Deleted; // "Trimmed."  Not actually stored on disk in CORFU, but this isn't CORFU.
    bool Speculative; // An update that should be ignored, but will be referenced in a (successful) commit record.
}

struct CommitRecord {
    Set<int> ReadOffsets; // Probably most convenient to represent these sets as sorted lists, earliest offset first.
    Set<int> WriteOffsets;
}

struct DecisionRecord {
    int CommitRecordOffset;
    bool Succeeded;
}

struct Checkpoint {
    Unique Oid;
    Opaque State;
    int? PreviousCheckpointOffset; // This isn't suggested by the paper, but it may make sense...
    // Generally, you could imagine arbitrary indexing if you want to be able to quickly rollback to any point in time.
}
```







### Layered Partitions

A minor nuisance when first reading is the placing of the failure handling sub-sections (in this and the next section).
As you are reading it becomes clear that the proposed changes violate earlier assumptions. These are addressed, but not
until the end of the sections.

### Streaming CORFU

### Evaluation

No real comments except that it would have been nice to see the latency and throughput figures with the batched sequencer
for various batch sizes.
