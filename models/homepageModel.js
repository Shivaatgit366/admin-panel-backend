import mongoose from 'mongoose';

const homePageSchema = new mongoose.Schema({
    title_1: String,
    title_2:String,
    title_3:String,
    buttonTitle_1: String,
    buttonLink_1:String,
    desktop_image: String,
    mobile_image:String,
    type:String
});

const homePage = mongoose.model("homePage", homePageSchema);
export default homePage;
