mod flags {
    use cql::def::*;

    #[test]
    fn bit_or() {
        let mut flags = 0;
        flags |= Flags::Compression;
        assert_eq!(flags, 0x01);

        flags |= Flags::Warning;
        assert_eq!(flags, 0x09);
    }

    #[test]
    fn bit_and() {
        let mut flags = 0;
        flags |= Flags::Compression;
        flags |= Flags::Beta;
        assert_eq!(flags, 0x11);

        flags &= !Flags::Compression;
        assert_eq!(flags, 0x10);
    }

    #[test]
    fn is_set() {
        let mut flags = 0;
        flags |= Flags::Compression;
        flags |= Flags::Beta;

        assert_eq!(true, Flags::Compression.is_set(flags));
        assert_eq!(true, Flags::Beta.is_set(flags));
        assert_eq!(false, Flags::Tracing.is_set(flags));
        assert_eq!(false, Flags::CustomPayload.is_set(flags));
        assert_eq!(false, Flags::Warning.is_set(flags));
    }
}

mod query_flags {
    use cql::def::*;

    #[test]
    fn bit_or() {
        let mut flags = 0;
        flags |= QueryFlags::Values;
        assert_eq!(flags, 0x01);

        flags |= QueryFlags::SkipMetadata;
        assert_eq!(flags, 0x03);
    }

    #[test]
    fn bit_and() {
        let mut flags = 0;
        flags |= QueryFlags::Values;
        flags |= QueryFlags::Keyspace;
        assert_eq!(flags, 0x81);

        flags &= !QueryFlags::Values;
        assert_eq!(flags, 0x80);
    }

    #[test]
    fn is_set() {
        let mut flags = 0;
        flags |= QueryFlags::Values;
        flags |= QueryFlags::SkipMetadata;

        assert_eq!(true, QueryFlags::Values.is_set(flags));
        assert_eq!(true, QueryFlags::SkipMetadata.is_set(flags));
    }
}
