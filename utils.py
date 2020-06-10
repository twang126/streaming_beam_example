import apache_beam as beam


def get_CalculateFeaturesPerFeatureCombineFn(tag):
    f = CalculateFeaturesRegion()
    f.set_tag(tag)

    return f

class CalculateFeaturesRegion(beam.CombineFn):
    def set_tag(self, tag):
        self.tag = tag
        
    def create_accumulator(self):
        return {}

    def add_input(self, accumulator, element):
        event_type = element['type']
        user_id = element['id']
        ts = element['ts']
        score = element['score']

        if event_type not in accumulator:
            accumulator[event_type] = {}

        if user_id not in accumulator[event_type]:
            accumulator[event_type][user_id] = (score, ts)
        else:
            existing = accumulator[event_type][user_id]

            existing_ts = existing[1]

            # Override least recent value
            if existing_ts < ts:
                accumulator[event_type][user_id] = (score, ts)
        
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()

        for acc in accumulators:
            for event_type,v in acc.items():
                if event_type not in merged:
                    merged[event_type] = {}

                for user_id, tup in v.items():
                    if user_id not in merged[event_type]:
                        merged[event_type][user_id] = tup
                    else:
                        existing = merged[event_type][user_id]

                        if existing[1] <  tup[1]:
                            merged[event_type][user_id] = tup
        
        return merged

    def extract_output(self, accumulator):
        return_val = {}

        for event_type, val in accumulator.items():
            return_val[event_type] = 0

            for _, values in val.items():
                return_val[event_type] += values[0]

        return_val['tag'] = self.tag
        return return_val