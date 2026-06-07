package parsers

// Parser classifies and extracts telemetry from a TCP payload.
type Parser interface {
	Protocol() string
	Matches(in ParseInput) bool
	Parse(in ParseInput) (Event, bool)
}

// Factory dispatches to the first parser that matches the input.
type Factory struct {
	parsers []Parser
}

func NewFactory() *Factory {
	return &Factory{
		parsers: []Parser{
			NewKafkaParser(),
			NewAmqpParser(),
		},
	}
}

func (f *Factory) Parse(in ParseInput) (Event, bool) {
	for _, parser := range f.parsers {
		if !parser.Matches(in) {
			continue
		}
		event, ok := parser.Parse(in)
		if ok {
			event.Protocol = parser.Protocol()
			return event, true
		}
	}
	return Event{}, false
}
