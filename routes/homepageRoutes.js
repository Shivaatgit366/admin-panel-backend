import express from "express";
import dotenv from "dotenv";
dotenv.config();
import { getHomePageData } from "../controllers/homePageController.js";
import homePage from "../models/homepageModel.js";
import multer from "multer";
import { httpStatusCodes } from "../controllers/errorController.js";
import {
  S3Client,
  DeleteObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";
import multerS3 from "multer-s3";
import { authenticateToken } from "../middlewares/auth.js";

//router object
const router = express.Router();

const s3 = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const upload = multer({
  storage: multerS3({
    s3: s3,
    bucket: process.env.AWS_S3_BUCKET_NAME,
    acl: "public-read",
    metadata: function (req, file, cb) {
      cb(null, { fieldName: file.fieldname });
    },
    key: function (req, file, cb) {
      const fileName = `${Date.now()}_${file.originalname}`;
      cb(null, `${fileName}`);
    },
  }),
});

//post api for upload dynamic banner data
router.post(
  "/upload",
  authenticateToken,
  upload.fields([
    { name: "desktop_image", maxCount: 1 },
    { name: "mobile_image", maxCount: 1 },
  ]),
  async (req, res) => {
    try {
      const allowedFields = [
        "title_1",
        "title_2",
        "title_3",
        "buttonTitle_1",
        "buttonLink_1",
        "type",
      ];

      // Dynamically construct updateFields from allowedFields
      const updateFields = allowedFields.reduce((fields, key) => {
        fields[key] = req.body[key] || "";
        return fields;
      }, {});

      const record = await homePage.find({ type: "homePage" });

      const deleteImageIfNeeded = async (fileName) => {
        const params = {
          Bucket: process.env.AWS_S3_BUCKET_NAME,
          Key: fileName, // Path to the file in S3
        };

        const command = new DeleteObjectCommand(params);
        const gg = await s3.send(command);
        console.log(`File deleted successfully: ${fileName}`);
      };

      if (
        req.body.desktop_image !== record[0].desktop_image &&
        req.files["desktop_image"]
      ) {
        // If there's an old desktop image, delete it
        if (record[0].desktop_image) {
          await deleteImageIfNeeded(record[0].desktop_image);
        }
        updateFields.desktop_image = req.files.desktop_image[0].key;
      } else if (req.body.desktop_image) {
        // If desktop_image is provided in the body
        updateFields.desktop_image = req.body.desktop_image;
      } else {
        updateFields.desktop_image = ""; // Clear the field if no image provided
        record[0].desktop_image &&
          (await deleteImageIfNeeded(record[0].desktop_image));
      }

      if (
        req.body.mobile_image !== record[0].mobile_image &&
        req.files["mobile_image"]
      ) {
        // If there's an old mobile image, delete it

        if (record[0].mobile_image) {
          await deleteImageIfNeeded(record[0].mobile_image);
        }
        updateFields.mobile_image = req.files.mobile_image[0].key;
      } else if (req.body.mobile_image) {
        // If mobile_image is provided in the body
        updateFields.mobile_image = req.body.mobile_image;
      } else {
        updateFields.mobile_image = ""; // Clear the field if no image provided
        record[0].mobile_image &&
          (await deleteImageIfNeeded(record[0].mobile_image));
      }

      updateFields.type = "homePage";

      const homepage = await homePage.findOneAndUpdate(
        { type: "homePage" }, // Static field to identify this document
        updateFields,
        { new: true, upsert: true }
      );

      res.status(201).json({
        status: httpStatusCodes.OK,
        success: true,
        message: "Homepage uploaded successfully",
        result: homepage,
      });
    } catch (err) {
      res
        .status(200)
        .json({ success: false, message: "Failed to upload banner" });
    }
  }
);

//get api for upload dynamic banner data
router.get("/upload", authenticateToken, getHomePageData);
router.get("/getImageList", async () => {
  try {
    const params = {
      Bucket: process.env.AWS_S3_BUCKET_NAME,
    };
    const command = new ListObjectsV2Command(params);
    const data = await s3.send(command);

    data.Contents.forEach((object) => {
      console.log("Key:", object.Key, "Size:", object.Size);
    });
  } catch (err) {
    console.error("Error listing objects:", err);
  }
});

export default router;
