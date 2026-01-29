N::Topic {
    name: String,
    category: String,
}

E::RelatesTo {
    From: Topic,
    To: Topic,
    Properties: {
        weight: F64,
    }
}

E::Supports {
    From: Topic,
    To: Topic,
}

E::Opposes {
    From: Topic,
    To: Topic,
}
