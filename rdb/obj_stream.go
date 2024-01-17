package rdb

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

const (
	streamItemFlagDeleted    = 1 << 0
	streamItemFlagSameFields = 1 << 1
)

type StreamObjectEvent struct {
	Key     string
	Entries []*StreamEntry
	Groups  []*StreamConsumerGroup
}

func (e *StreamObjectEvent) Debug() {
	fmt.Printf("=== StreamObjectEvent ===\n")
	fmt.Printf("Key: %s\n", e.Key)

	fmt.Printf("Entry size: %d\n", len(e.Entries))
	fmt.Printf("Entries:\n")
	for _, e := range e.Entries {
		id := fmt.Sprintf("%d-%d", e.Id.Ms, e.Id.Seq)
		var fields []string
		for field, value := range e.Fields {
			fields = append(fields, fmt.Sprintf("%s=%s", field, value))
		}
		fmt.Printf("\tid=%s fields=%s\n", id, strings.Join(fields, ","))
	}

	fmt.Printf("Group size: %d\n", len(e.Groups))
	if len(e.Groups) > 0 {
		fmt.Printf("Groups:\n")
		for _, g := range e.Groups {
			fmt.Printf("\tname=%s\n", g.Name)
		}
	}

	fmt.Printf("\n")
}

type StreamEntry struct {
	Id     StreamId
	Fields map[string]string
}

type StreamConsumerGroup struct {
	Name string

	// Last delivered (not acknowledged) ID for this group.
	// Consumers that will just ask for more messages will served with IDs > than this.
	LastId StreamId

	PEL       []*StreamNAck
	Consumers []*StreamConsumer
}

type StreamConsumer struct {
	// Last time this consumer was active.
	SeenTime uint64

	Name string

	// Consumer specific pending entries list: all the pending messages delivered to this
	// consumer not yet acknowledged.
	PEL []*StreamNAck
}

type StreamNAck struct {
	Id StreamId

	// Last time this message was delivered.
	DeliveryTime uint64

	// Number of times this message was delivered.
	DeliveryCount uint64

	// The consumer this message was delivered to in the last delivery.
	Consumer *StreamConsumer
}

type StreamId struct {
	Ms  uint64
	Seq uint64
}

func parseStream(key string, r *rdbReader, valueType byte) (*StreamObjectEvent, error) {
	stream := &StreamObjectEvent{Key: key}
	switch valueType {
	case valueTypeStreamListPacks, valueTypeListQuickList2:
		return parseStream0(r, valueType, stream)
	default:
		return nil, fmt.Errorf("unsupported stream rdb type: 0x%x", valueType)
	}
}

// t_stream.c::streamAppendItem
func parseStream0(r *rdbReader, valueType byte, stream *StreamObjectEvent) (*StreamObjectEvent, error) {
	listPackSize, err := r.GetLengthInt()
	if err != nil {
		return nil, err
	}

	for i := 0; i < listPackSize; i++ {
		/* Get the master ID, the one we'll use as key of the radix tree
		 * node: the entries inside the listpack itself are delta-encoded
		 * relatively to this ID. */
		nodeKey, err := r.GetLengthBytes()
		if err != nil {
			return nil, err
		}
		masterMs := binary.BigEndian.Uint64(nodeKey[:8])
		masterSeq := binary.BigEndian.Uint64(nodeKey[8:])

		// Parse list pack
		members, err := parseListPack(r)
		if err != nil {
			return nil, err
		}

		// Parse master entry from list pack members.
		//
		// +-------+---------+------------+---------+--/--+---------+---------+-+
		// | count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
		// +-------+---------+------------+---------+--/--+---------+---------+-+
		//
		mIndex := 0
		validCount, err := strconv.ParseInt(members[mIndex], 10, 64)
		if err != nil {
			return nil, err
		}
		mIndex++

		deletedCount, err := strconv.ParseInt(members[mIndex], 10, 64)
		if err != nil {
			return nil, err
		}
		mIndex++

		masterNumFields, err := strconv.ParseInt(members[mIndex], 10, 64)
		if err != nil {
			return nil, err
		}
		mIndex++

		masterFields := members[mIndex : mIndex+int(masterNumFields)]
		mIndex += int(masterNumFields)

		if last := members[mIndex]; last != "0" {
			return nil, fmt.Errorf("stream master entry must end of '0', not %s", last)
		}
		mIndex++

		for i := int64(0); i < validCount+deletedCount; i++ {
			//
			// +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
			// |flags|entry-id|num-fields|field-1|value-1|...|field-N|value-N|lp-count|
			// +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
			//
			// However if the SAMEFIELD flag is set, we have just to populate
			// the entry with the values, so it becomes:
			//
			// +-----+--------+-------+-/-+-------+--------+
			// |flags|entry-id|value-1|...|value-N|lp-count|
			// +-----+--------+-------+-/-+-------+--------+

			flags, err := strconv.ParseUint(members[mIndex], 10, 64)
			if err != nil {
				return nil, err
			}
			mIndex++

			entryIdMs, err := strconv.ParseUint(members[mIndex], 10, 64)
			if err != nil {
				return nil, err
			}
			mIndex++

			entryIdSeq, err := strconv.ParseUint(members[mIndex], 10, 64)
			if err != nil {
				return nil, err
			}
			mIndex++

			entry := &StreamEntry{
				Id: StreamId{
					Ms:  entryIdMs + masterMs,
					Seq: entryIdSeq + masterSeq,
				},
			}

			if flags&streamItemFlagSameFields == 0 {
				nFields, err := strconv.ParseInt(members[mIndex], 10, 64)
				if err != nil {
					return nil, err
				}
				mIndex++
				entry.Fields = make(map[string]string, int(nFields))
				for i := 0; i < int(nFields); i++ {
					field := members[mIndex]
					value := members[mIndex+1]
					mIndex += 2
					entry.Fields[field] = value
				}
			} else {
				entry.Fields = make(map[string]string, int(masterNumFields))
				for i := 0; i < int(masterNumFields); i++ {
					field := masterFields[i]
					value := members[mIndex]
					mIndex++
					entry.Fields[field] = value
				}
			}

			_ = members[mIndex]
			mIndex++

			if flags&streamItemFlagDeleted == 0 {
				stream.Entries = append(stream.Entries, entry)
			}
		}
	}

	// Load total number of items inside the stream.
	// Current number of elements inside this stream.
	streamSize, err := r.GetLengthInt()
	if err != nil {
		return nil, err
	}
	_ = streamSize

	// Load the last entry ID.
	// Zero if there are yet no items.
	lastIdMs, err := r.GetLengthUInt64()
	if err != nil {
		return nil, err
	}
	lastIdSeq, err := r.GetLengthUInt64()
	if err != nil {
		return nil, err
	}
	_ = lastIdMs
	_ = lastIdSeq

	if valueType == valueTypeStreamListPacks2 {
		// Load the first entry ID.
		// The first non-tombstone entry, zero if empty.
		firstIdMs, err := r.GetLengthUInt64()
		if err != nil {
			return nil, err
		}
		firstIdSeq, err := r.GetLengthUInt64()
		if err != nil {
			return nil, err
		}
		_ = firstIdMs
		_ = firstIdSeq

		// Load the maximal deleted entry ID.
		// The maximal ID that was deleted.
		maxDeletedEntryIdMs, err := r.GetLengthUInt64()
		if err != nil {
			return nil, err
		}
		maxDeletedEntryIdSeq, err := r.GetLengthUInt64()
		if err != nil {
			return nil, err
		}
		_ = maxDeletedEntryIdMs
		_ = maxDeletedEntryIdSeq

		// Load the offset.
		// All time count of elements added.
		entriesAdded, err := r.GetLengthUInt64()
		if err != nil {
			return nil, err
		}
		_ = entriesAdded
	} else {
		// During migration the offset can be initialized to the stream's
		// length. At this point, we also don't care about tombstones
		// because CG offsets will be later initialized as well.

		// Not load from reader, so do nothing.
	}

	// Consumer groups loading
	consumerGroupCount, err := r.GetLengthInt()
	if err != nil {
		return nil, err
	}
	for i := 0; i < consumerGroupCount; i++ {
		// Get the consumer group name and ID. We can then create the
		// consumer group ASAP and populate its structure as
		// we read more data.
		cg := new(StreamConsumerGroup)
		groupName, err := r.GetLengthString()
		if err != nil {
			return nil, err
		}
		cg.Name = groupName

		groupLastIdMs, err := r.GetLengthUInt64()
		if err != nil {
			return nil, err
		}
		groupLastIdSeq, err := r.GetLengthUInt64()
		if err != nil {
			return nil, err
		}
		cg.LastId = StreamId{
			Ms:  groupLastIdMs,
			Seq: groupLastIdSeq,
		}

		// Load group offset.
		var groupOffset uint64
		if valueType == valueTypeStreamListPacks2 {
			groupOffset, err = r.GetLengthUInt64()
			if err != nil {
				return nil, err
			}
		} else {
			// Not load offset from reader, so do nothing.
		}
		_ = groupOffset

		// Load the global PEL for this consumer group, however we'll
		// not yet populate the NACK structures with the message
		// owner, since consumers for this group and their messages will
		// be read as a next step. So for now leave them not resolved
		// and later populate it.
		pelSize, err := r.GetLengthInt()
		if err != nil {
			return nil, err
		}
		//pelMap := make(map[StreamId]*StreamNAck, pelSize)
		pelMap := make(map[string]*StreamNAck, pelSize)
		for i := 0; i < pelSize; i++ {
			pelIdMs, err := r.GetBUint64()
			if err != nil {
				return nil, err
			}
			pelIdSeq, err := r.GetBUint64()
			if err != nil {
				return nil, err
			}
			pelDeliveryTime, err := r.GetLUint64()
			if err != nil {
				return nil, err
			}
			pelDeliveryCount, err := r.GetLengthUInt64()
			if err != nil {
				return nil, err
			}
			pel := &StreamNAck{
				Id: StreamId{
					Ms:  pelIdMs,
					Seq: pelIdSeq,
				},
				DeliveryTime:  pelDeliveryTime,
				DeliveryCount: pelDeliveryCount,
				Consumer:      nil,
			}
			cg.PEL = append(cg.PEL, pel)
			pelMap[fmt.Sprintf("%d-%d", pelIdMs, pelIdSeq)] = pel
		}

		// Now that we loaded our global PEL, we need to load the
		// consumers and their local PELs.
		consumerNum, err := r.GetLengthUInt64()
		if err != nil {
			return nil, err
		}
		for i := 0; i < int(consumerNum); i++ {
			c := new(StreamConsumer)
			name, err := r.GetLengthString()
			if err != nil {
				return nil, err
			}
			c.Name = name

			// Last time this consumer was active
			seenTime, err := r.GetLUint64()
			if err != nil {
				return nil, err
			}
			c.SeenTime = seenTime

			// Load the PEL about entries owned by this specific consumer.
			pelSize, err := r.GetLengthUInt64()
			if err != nil {
				return nil, err
			}

			for i := 0; i < int(pelSize); i++ {
				consumerPelIdMs, err := r.GetBUint64()
				if err != nil {
					return nil, err
				}
				consumerPelIdSeq, err := r.GetBUint64()
				if err != nil {
					return nil, err
				}
				messageId := fmt.Sprintf("%d-%d", consumerPelIdMs, consumerPelIdSeq)
				pel, ok := pelMap[messageId]
				if !ok {
					return nil, fmt.Errorf("consumer pel not found in global pel")
				}
				pel.Consumer = c
				c.PEL = append(c.PEL, pel)
			}
			cg.Consumers = append(cg.Consumers, c)
		}
		stream.Groups = append(stream.Groups, cg)
	}

	return stream, nil
}
