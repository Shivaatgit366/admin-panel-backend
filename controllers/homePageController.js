import homePage from "../models/homepageModel.js";
import { tryCatchFn, httpStatusCodes } from "./errorController.js";

// const multer = require("multer");

// const storage = multer.diskStorage({
//     destination: (req, file, cb) => {
//         cb(null, "uploads/");
//     },
//     filename: (req, file, cb) => {
//         cb(null, `${Date.now()}-${file.originalname}`);
//     },
// });
// const upload = multer({ storage });



const imageUpload = tryCatchFn(async (req, res) => {
    res.status(201).json({ message: "Banner uploaded successfully" });

})


const getHomePageData = tryCatchFn(async (req, res) => {
    const result = await homePage.find({}, '-_id -__v');
    if (result.length > 0) {
        res.status(200).json({ status: httpStatusCodes.OK, success: true, message: "Data fetched successfully.", result: result });
    } else {
        res.json({ status: 404, success: false, message: "Homepage Banner Not Available" });
    }

})

export { imageUpload, getHomePageData }
