namespace scala io.sqooba.oss.timeseries.thrift

/**
 * Binary format for the footer of a MultiSeriesBlock.
 */
struct TMultiSeriesFooter {
    /**
     * Version number of the format
     */
    0: required i32                 version,

    /**
     * Offsets in the blob of the GorillaSuperBlocks
     */
    1: required list<i64>           offsets,

    /**
     * Optionally maps string keys/names to GorillaSuperBlocks. The integers are
     * the indexes of the 'offsets' list.
     */
    2: optional map<string, i32>    keys
}

// Singleton constant for the TupleGorillaBlock implementation
struct TTupleBlockType {}
const TTupleBlockType TTupleBlock = {}

// Type for the SampledGorillaBlock implementation
struct TSampledBlockType {
    0: required i64     sampleRate
}

/**
 * Different implementation types of the GorillaBlock, as a union in order to
 * support arguments for the types.
 */
union TBlockType {
    0: TTupleBlockType      tuple = TTupleBlock,
    1: TSampledBlockType    sample
}

/**
 * Binary format for the metadata stored in the footer of a GorillaSuperBlock.
 */
struct TSuperBlockMetadata {
    /**
     * Version number of the format
     */
    0: required i32 version,

    1: required TBlockType blockType,

    // TODO
    // i: time unit, how to store their identifiers?
    // j: numeric type
}
