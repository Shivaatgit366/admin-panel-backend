import axios from "axios";
import dotenv from 'dotenv';
dotenv.config();

const graphqlClient = async (query, variables = {}) => {
    const API_URL = process.env.SHOPIFY_API_URL;
    const ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;

    try {
        const response = await axios.post(
            API_URL,
            { query, variables },
            {
                headers: {
                    'Content-Type': 'application/json',
                    'X-Shopify-Access-Token': ACCESS_TOKEN, // Use X-Shopify-Access-Token
                    'User-Agent': 'YourAppName/1.0', // Optional, for Shopify logging purposes
                },
            }
        );

        const { data, errors } = response.data;
        if (errors) {
            console.error("GraphQL Errors:", JSON.stringify(errors));
            return { success: false, data: null, error: errors };
        }

        return { success: true, data, error: null };
    } catch (error) {
        console.error("Axios Error:", error.response?.data || error.message);
        return { success: false, data: null, error: error.response?.data || error.message };
    }
};

export { graphqlClient };