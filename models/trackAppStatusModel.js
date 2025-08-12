import mongoose from 'mongoose';

const trackAppStatusSchema = new mongoose.Schema({
    last_modified: {
        type: String,
        required: [true, 'last_modified is required field!'],
    },

    // Define the properties of the nested object here
    status: {
        type: Number,
        required: [true, 'status is required field!'],
    },
    success: {
        type: Boolean,
        required: [true, 'success is required field!'],
    },
    message: {
        type: String,
        required: [true, 'message is required field!'],
    }

});

const trackAppStatus = mongoose.model("trackAppStatus", trackAppStatusSchema);
export default trackAppStatus;

