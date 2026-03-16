

class LSMStripState(BaseModel):
    slice_id: int
    strip_id: int
    camera_id: str
    state: str
    timestamp: datetime

    def mark_started(self):
        self.state = "started"
        self.timestamp = datetime.now()
