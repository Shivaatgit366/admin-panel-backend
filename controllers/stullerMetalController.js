import { httpStatusCodes } from "./errorController.js";
import sqldb from "../mysqldb.js";
import { graphqlClient } from "../helpers/commonFnHelper.js";
import Joi from "joi";
import axios from "axios";
import FormData from "form-data";

const getMetal = async (req, res) => {
  try {
    // Define Joi schema for request body validation
    const schema = Joi.object({
      sort: Joi.string().valid("ASC", "DESC").default("ASC"),
    });

    // Validate request body
    const { error, value } = schema.validate(req.body);

    if (error) {
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: error.details[0].message,
      });
    }

    const { sort } = value;

    // Fetch all metals with sorting
    const query = `
      SELECT 
        m.*
      FROM stlr_metals m
      ORDER BY m.name ${sort};
    `;

    const [metals] = await sqldb.query(query);

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Metals fetched successfully.",
      result: metals,
    });
  } catch (error) {
    console.error("Error fetching metals:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
};

const getMetalsByIds = async (req, res) => {
  const schema = Joi.object({
    metal_ids: Joi.array()
      .items(Joi.number().integer().positive().required())
      .min(1)
      .required()
      .messages({
        "array.base": "metal_ids must be an array.",
        "array.min": "At least one metal_id must be provided.",
        "any.required": "metal_ids array is required.",
      }),
  });

  const { error } = schema.validate(req.body);
  if (error) {
    return res.status(400).json({
      status: 400,
      success: false,
      message: error.details[0].message,
    });
  }

  const { metal_ids } = req.body;

  const connection = await sqldb.getConnection();
  try {
    const [metals] = await connection.execute(
      `SELECT * FROM wedding_band.stlr_metals 
       WHERE metal_id IN (${metal_ids.map(() => "?").join(", ")})`,
      metal_ids
    );

    return res.status(200).json({
      status: 200,
      success: true,
      message: "Metals fetched successfully.",
      result: metals,
    });
  } catch (error) {
    console.error("Error fetching metals:", error);
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal Server Error",
    });
  } finally {
    connection.release();
  }
};

const createMetal = async (req, res) => {
  // Joi schema for metal_name validation
  const metalSchema = Joi.object({
    metal_name: Joi.string()
      .trim()
      .min(2)
      .max(100)
      .regex(/^(?!\d+$).*$/) // Prevents purely numeric names
      .required()
      .messages({
        "string.empty": "Metal name is required.",
        "string.min": "Metal name must be at least 2 characters long.",
        "string.max": "Metal name cannot exceed 100 characters.",
        "string.pattern.base": "Metal name cannot be only numbers.",
      }),
  });

  const { error } = metalSchema.validate(req.body);
  if (error) {
    return res.status(400).json({
      status: httpStatusCodes.NOT_FOUND,
      success: false,
      message: error.details[0].message,
    });
  }

  const { metal_name } = req.body;
  const files = req.files || [];

  if (!metal_name) {
    return res.status(400).json({
      status: httpStatusCodes.NOT_FOUND,
      success: false,
      message: "Metal name is required",
    });
  }

  // Create a database connection
  const connection = await sqldb.getConnection();

  try {
    await connection.beginTransaction();

    // Check if metal name already exists
    const [existingMetal] = await connection.execute(
      `SELECT metal_id FROM wedding_band.stlr_metals WHERE name = ?`,
      [metal_name]
    );

    if (existingMetal.length > 0) {
      return res.status(409).json({
        status: 409,
        success: false,
        message: "Metal name already exists",
      });
    }

    /**
     * Insert the new metal into the metafield "Metal" predefined list
     */
    const METAFIELD_DEFINITIONS_QUERY = `
      query {
        metafieldDefinitions(first: 100, ownerType: PRODUCT) {
          edges {
            node {
              id
              name
              namespace
              key
              validations {
                name
                value
              }
            }
          }
        }
      }
    `;

    const response1 = await graphqlClient(METAFIELD_DEFINITIONS_QUERY);
    const definitions = response1?.data?.metafieldDefinitions?.edges?.map(
      (edge) => edge.node
    );

    const metalNameMetafield = definitions.find((def) => def.name === "Metal");
    if (!metalNameMetafield) {
      throw new Error("Metal Name metafield definition not found");
    }

    const metalNameMetafieldId = metalNameMetafield.id;

    const METAFIELD_PREDEFINED_VALUES_QUERY = `
      query {
        metafieldDefinition(id: "${metalNameMetafieldId}") {
          id
          name
          namespace
          key
          validations {
            name
            value
          }
        }
      }
    `;

    const response2 = await graphqlClient(METAFIELD_PREDEFINED_VALUES_QUERY);
    const definition =
      response2?.data?.metafieldDefinition ?? response2?.metafieldDefinition;
    const validations = definition?.validations || [];

    const choicesValidation = validations.find((v) => v.name === "choices");
    let currentChoices = [];

    if (choicesValidation && choicesValidation.value) {
      try {
        currentChoices = JSON.parse(choicesValidation.value);
      } catch (e) {
        console.error(
          "Invalid choices JSON format, initializing to empty list."
        );
        currentChoices = [];
      }
    }

    if (!currentChoices.includes(metal_name)) {
      currentChoices.push(metal_name);
    }

    const UPDATE_ENUM_VALUES_MUTATION = `
      mutation metafieldDefinitionUpdate($definition: MetafieldDefinitionUpdateInput!) {
        metafieldDefinitionUpdate(definition: $definition) {
          updatedDefinition {
            id
            validations {
              name
              value
            }
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const variables = {
      definition: {
        namespace: metalNameMetafield.namespace,
        key: metalNameMetafield.key,
        ownerType: "PRODUCT",
        validations: [
          {
            name: "choices",
            value: JSON.stringify(currentChoices),
          },
        ],
      },
    };

    const updateResponse = await graphqlClient(
      UPDATE_ENUM_VALUES_MUTATION,
      variables
    );

    if (updateResponse?.errors) {
      console.error("GraphQL errors:", updateResponse.errors);
      throw new Error("GraphQL mutation failed");
    }

    const userErrors =
      updateResponse?.data?.metafieldDefinitionUpdate?.userErrors;
    if (userErrors && userErrors.length > 0) {
      console.error("Shopify update errors:", userErrors);
      throw new Error("Failed to update Shopify metal list");
    }

    /**
     * Update predefined choices for the field "Metal" in the metaobject definition "Sub Collection Urls".
     */
    const GET_METAOBJECT_DEFINITIONS = `
      query {
        metaobjectDefinitions(first: 100) {
          edges {
            node {
              id
              name
              fieldDefinitions {
                key
                name
                type {
                  name
                }
                required
                validations {
                  name
                  value
                }
              }
            }
          }
        }
      }
    `;

    // Fetch all metaobject definitions
    const metaobjectDefsResponse = await graphqlClient(
      GET_METAOBJECT_DEFINITIONS
    );

    const metaobjectDefs =
      metaobjectDefsResponse?.data?.metaobjectDefinitions?.edges?.map(
        (edge) => edge.node
      );

    if (!metaobjectDefs || metaobjectDefs.length === 0) {
      throw new Error("No metaobject definitions found");
    }

    // Find the "Sub Collection Urls" metaobject definition by name
    const subCollectionMetaobjectDef = metaobjectDefs.find(
      (def) => def.name === "Sub Collection Urls"
    );

    if (!subCollectionMetaobjectDef) {
      throw new Error('"Sub Collection Urls" metaobject definition not found');
    }

    const styleField = subCollectionMetaobjectDef.fieldDefinitions.find(
      (field) => field.key === "metal"
    );

    if (!styleField) {
      throw new Error(
        'Metal field not found in "Sub Collection Urls" metaobject'
      );
    }

    let currentChoicesForStylefield = [];

    const choicesValidationForStyle = styleField.validations.find(
      (v) => v.name === "choices"
    );

    if (choicesValidationForStyle && choicesValidationForStyle.value) {
      try {
        currentChoicesForStylefield = JSON.parse(
          choicesValidationForStyle.value
        );
      } catch (e) {
        console.warn(
          "Invalid choices JSON in style field, resetting to empty array."
        );
        currentChoicesForStylefield = [];
      }
    }

    // Add new metal_name if not already included
    if (!currentChoicesForStylefield.includes(metal_name)) {
      currentChoicesForStylefield.push(metal_name);
    }

    // Preserve other validations (excluding "choices")
    const otherValidations = styleField.validations.filter(
      (v) => v.name !== "choices"
    );

    // Add updated choices validation
    const updatedValidations = [
      ...otherValidations,
      {
        name: "choices",
        value: JSON.stringify(currentChoicesForStylefield),
      },
    ];

    const UPDATE_METAOBJECT_DEFINITION = `
      mutation UpdateMetaobjectDefinition($id: ID!, $definition: MetaobjectDefinitionUpdateInput!) {
        metaobjectDefinitionUpdate(id: $id, definition: $definition) {
          metaobjectDefinition {
            id
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const definitionInput = {
      fieldDefinitions: {
        update: {
          key: "metal",
          validations: updatedValidations,
        },
      },
    };

    const updateResponseForMetaObject = await graphqlClient(
      UPDATE_METAOBJECT_DEFINITION,
      {
        id: subCollectionMetaobjectDef.id,
        definition: definitionInput,
      }
    );

    const errors =
      updateResponseForMetaObject?.data?.metaobjectDefinitionUpdate?.userErrors;

    if (errors && errors.length > 0) {
      throw new Error(`Failed to update field: ${JSON.stringify(errors)}`);
    } else {
      console.log("Metal field updated successfully.");
    }

    /**
     * If there is no file uploaded, then create a metaobject with the metal name only.
     */
    if (files.length === 0) {
      const CREATE_METAOBJECT_MUTATION = `
        mutation CreateMetaobject($metaobject: MetaobjectCreateInput!) {
          metaobjectCreate(metaobject: $metaobject) {
            metaobject {
              id
              type
              fields {
                key
                value
              }
            }
            userErrors {
              field
              message
            }
          }
        }
      `;

      const metaobjectInput = {
        type: "filter_images",
        fields: [
          { key: "name", value: metal_name },
          { key: "type", value: "Metal" },
          // No image field since no file was uploaded
        ],
      };

      const metaobjectResponse = await graphqlClient(
        CREATE_METAOBJECT_MUTATION,
        { metaobject: metaobjectInput }
      );

      if (
        metaobjectResponse?.data?.metaobjectCreate?.userErrors &&
        metaobjectResponse.data.metaobjectCreate.userErrors.length > 0
      ) {
        throw new Error(
          `Metaobject creation failed: ${JSON.stringify(
            metaobjectResponse.data.metaobjectCreate.userErrors
          )}`
        );
      }
    } else if (files.length > 0) {
      /**
       * If the file is uploaded, then upload the file to the Shopify and create a metaobject with the metal name and image url.
       */
      files.forEach((file) => {
        const extension = file.originalname.split(".").pop();
        file.renamedTo = `${metal_name}.${extension}`;
      });

      // This api takes only one image only. So we will take the first image from the files array.
      const file = files[0];

      /**
       * Upload the file to Shopify using Staged Uploads API and get the URL.
       */
      const stagedUploadsMutation = `
          mutation generateStagedUploadTarget($input: [StagedUploadInput!]!) {
            stagedUploadsCreate(input: $input) {
              stagedTargets {
                url
                resourceUrl
                parameters {
                  name
                  value
                }
              }
              userErrors {
                field
                message
              }
            }
          }
        `;

      const uploadInput = [
        {
          filename: file.renamedTo,
          mimeType: file.mimetype,
          resource: "FILE",
          httpMethod: "POST",
        },
      ];

      // Get the upload URL and parameters from Shopify
      const stagedUploadResponse = await graphqlClient(stagedUploadsMutation, {
        input: uploadInput,
      });

      const stagedTarget =
        stagedUploadResponse?.data?.stagedUploadsCreate?.stagedTargets?.[0];

      if (!stagedTarget) {
        throw new Error("Failed to get staged upload target from Shopify");
      }

      // Prepare form-data to upload the file buffer
      const form = new FormData();
      for (const param of stagedTarget.parameters) {
        form.append(param.name, param.value);
      }
      form.append("file", file.buffer, file.renamedTo);

      // Upload the file to Shopify's storage
      await axios.post(stagedTarget.url, form, {
        headers: form.getHeaders(),
        maxBodyLength: Infinity,
      });

      // Save the uploaded image URL for later use
      file.uploadedUrl = stagedTarget.resourceUrl;

      /**
       * Create the file object using the uploaded file URL. Then use it for the metaobject creation.
       */
      const FILE_CREATE_MUTATION = `
        mutation fileCreate($files: [FileCreateInput!]!) {
          fileCreate(files: $files) {
            files {
              id
            }
            userErrors {
              field
              message
            }
          }
        }
      `;

      const filesCreateInput = [
        {
          originalSource: file.uploadedUrl, // This is the resourceUrl from staged upload
          contentType: "IMAGE",
          alt: file.originalname,
        },
      ];

      // Create the file object in Shopify and get the file ID
      const filesCreateResponse = await graphqlClient(FILE_CREATE_MUTATION, {
        files: filesCreateInput,
      });

      // Adjust response path for your mutation name
      const createdFile = filesCreateResponse?.data?.fileCreate?.files?.[0];
      const fileId = createdFile?.id;

      // Wait 3 second for Shopify to index the file
      await new Promise((resolve) => setTimeout(resolve, 3000));

      // Use nodes(ids: $ids) to fetch the permanent URL
      const FILE_QUERY = `
        query getFile($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on MediaImage {
              id
              image {
                url
              }
            }
            ... on GenericFile {
              id
              url
            }
          }
        }
      `;

      const fileQueryResponse = await graphqlClient(FILE_QUERY, {
        ids: [fileId],
      });

      const fileNode = fileQueryResponse?.data?.nodes?.[0];

      let permanentUrl = null;

      if (fileNode?.image?.url) {
        permanentUrl = fileNode.image.url;
      } else if (fileNode?.url) {
        permanentUrl = fileNode.url;
      }

      if (!permanentUrl) {
        throw new Error(
          "Failed to fetch permanent URL for uploaded file from Shopify"
        );
      }

      // Use the permanent URL for DB and further logic
      file.uploadedUrl = permanentUrl;

      /**
       * Create the metaobject with the metal name and image url.
       */
      const CREATE_METAOBJECT_MUTATION = `
        mutation CreateMetaobject($metaobject: MetaobjectCreateInput!) {
          metaobjectCreate(metaobject: $metaobject) {
            metaobject {
              id
              type
              fields {
                key
                value
              }
            }
            userErrors {
              field
              message
            }
          }
        }
      `;

      const metaobjectInput = {
        type: "filter_images",
        fields: [
          { key: "name", value: metal_name },
          { key: "type", value: "Metal" },
          { key: "image", value: fileId },
        ],
      };

      const metaobjectResponse = await graphqlClient(
        CREATE_METAOBJECT_MUTATION,
        { metaobject: metaobjectInput }
      );

      if (
        metaobjectResponse?.data?.metaobjectCreate?.userErrors &&
        metaobjectResponse.data.metaobjectCreate.userErrors.length > 0
      ) {
        throw new Error(
          `Metaobject creation failed: ${JSON.stringify(
            metaobjectResponse.data.metaobjectCreate.userErrors
          )}`
        );
      }

      const createdMetaobject =
        metaobjectResponse?.data?.metaobjectCreate?.metaobject;
    }

    /**
     * Insert into database
     */
    const slug = metal_name.toLowerCase();

    const [result] = await connection.execute(
      `INSERT INTO wedding_band.stlr_metals (name, slug, image) VALUES (?, ?, ?)`,
      [metal_name, slug, files.length > 0 ? files[0].uploadedUrl : null]
    );

    await connection.commit();

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Metal created successfully",
      result: result.insertId,
    });
  } catch (error) {
    console.error("Error creating metal:", error);
    return res.status(500).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: "Internal Server Error",
    });
  } finally {
    connection.release();
  }
};

const updateSingleMetal = async (req, res) => {
  const updateMetalSchema = Joi.object({
    id: Joi.number().integer().positive().required().messages({
      "number.base": "Metal ID must be a number.",
      "number.integer": "Metal ID must be an integer.",
      "number.positive": "Metal ID must be a positive number.",
      "any.required": "Metal ID is required.",
    }),
  });

  const metalNameSchema = Joi.object({
    metal_name: Joi.string()
      .trim()
      .min(2)
      .max(100)
      .regex(/^(?!\d+$).*$/)
      .required()
      .messages({
        "string.empty": "Metal name is required.",
        "string.min": "Metal name must be at least 2 characters long.",
        "string.max": "Metal name cannot exceed 100 characters.",
        "string.pattern.base": "Metal name cannot be only numbers.",
      }),
    existing_url: Joi.string().allow("").required().messages({
      "string.base": "Existing URL must be a string.",
      "any.required": "Existing URL is required.",
    }),
  });

  const { id } = req.params;
  const numericId = Number(id);

  const { error: idError } = updateMetalSchema.validate({ id: numericId });
  if (idError) {
    return res.status(400).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: idError.details[0].message,
    });
  }

  const { error: nameError } = metalNameSchema.validate(req.body);
  if (nameError) {
    return res.status(400).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: nameError.details[0].message,
    });
  }

  const { metal_name } = req.body;
  const files = req.files || [];
  let existing_url = req.body.existing_url;

  const connection = await sqldb.getConnection();

  try {
    await connection.beginTransaction();

    // Check if metal exists
    const [metals] = await connection.query(
      "SELECT * FROM stlr_metals WHERE metal_id = ? LIMIT 1",
      [numericId]
    );

    if (metals.length === 0) {
      await connection.rollback();
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Metal not found. Please provide a valid metal ID.",
      });
    }

    const oldMetalName = metals[0].name;
    const oldImageUrl = metals[0].image;

    // Check for duplicate metal name
    const [existingMetal] = await connection.query(
      "SELECT metal_id FROM stlr_metals WHERE name = ? AND metal_id != ? LIMIT 1",
      [metal_name, numericId]
    );

    if (existingMetal.length > 0) {
      await connection.rollback();
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: "Metal name already exists. Please choose a different name.",
      });
    }

    // Validate that existing_url is either empty string or matches oldImageUrl
    if (existing_url !== "" && existing_url !== oldImageUrl) {
      await connection.rollback();
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: "existing_url must be empty or match the current image URL.",
      });
    }

    /**
     * Keep the filtered metaobjects of type "filter_images". This list is kept globally for name update and image update
     */
    const METAOBJECTS_BY_TYPE_QUERY = `
        query MetaobjectsByType($type: String!) {
          metaobjects(first: 100, type: $type) {
            edges {
              node {
                id
                type
                fields {
                  key
                  value
                }
              }
            }
          }
        }
    `;

    const metaobjectsResponse = await graphqlClient(METAOBJECTS_BY_TYPE_QUERY, {
      type: "filter_images",
    });

    const metaobjects =
      metaobjectsResponse?.data?.metaobjects?.edges?.map((edge) => edge.node) ||
      [];

    // Filter metaobjects whose "name" field matches oldMetalName and "type" field is "Metal"
    const filteredMetaobjects = metaobjects.filter((metaobject) => {
      if (!Array.isArray(metaobject.fields)) return false;
      const nameField = metaobject.fields.find((f) => f.key === "name");
      const typeField = metaobject.fields.find((f) => f.key === "type");
      return (
        nameField &&
        typeof nameField.value === "string" &&
        nameField.value.trim() === oldMetalName.trim() &&
        typeField &&
        typeof typeField.value === "string" &&
        typeField.value.trim() === "Metal"
      );
    });

    // If no metaobjects found, it means manually someone has deleted the metaobject. We will throw the error.
    if (filteredMetaobjects.length === 0) {
      await connection.rollback();
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: `You tried to update ${oldMetalName}. No metaobject found in the shopify with ${oldMetalName}. Can not be updated. Data mismatch. Please contact support.`,
      });
    }

    /**
     * Perform the shopify operations only if the new metal name and old metal name are different
     */
    if (oldMetalName.trim() !== metal_name.trim()) {
      /**
       * Update the metal name in the predefined style names list.
       */
      // Get the metal Name metafield id
      const METAFIELD_DEFINITIONS_QUERY = `
        query {
          metafieldDefinitions(first: 100, ownerType: PRODUCT) {
            edges {
              node {
                id
                name
                namespace
                key
                validations {
                  name
                  value
                }
              }
            }
          }
        }
      `;

      const response1 = await graphqlClient(METAFIELD_DEFINITIONS_QUERY);
      const definitions = response1?.data?.metafieldDefinitions?.edges?.map(
        (edge) => edge.node
      );
      const metalMetafield = definitions.find((def) => def.name === "Metal");

      if (!metalMetafield) {
        throw new Error("Metal metafield definition not found");
      }

      const metalMetafieldId = metalMetafield.id;

      // Get the existing metal names from the metafield "validations"
      const METAFIELD_PREDEFINED_VALUES_QUERY = `
        query {
          metafieldDefinition(id: "${metalMetafieldId}") {
            id
            name
            namespace
            key
            validations {
              name
              value
            }
          }
        }
      `;

      const response2 = await graphqlClient(METAFIELD_PREDEFINED_VALUES_QUERY);
      const definition =
        response2?.data?.metafieldDefinition ?? response2?.metafieldDefinition;
      const validations = definition?.validations || [];

      const choicesValidation = validations.find((v) => v.name === "choices");
      let currentChoices = [];

      if (choicesValidation && choicesValidation.value) {
        try {
          currentChoices = JSON.parse(choicesValidation.value);
        } catch (e) {
          currentChoices = [];
        }
      }

      // Replace the old name with the new metal name in the choices array
      let updatedChoices = currentChoices.map((choice) =>
        choice === oldMetalName ? metal_name : choice
      );

      updatedChoices = [...new Set(updatedChoices)];

      // Update the metafield definition with the new list of metal names
      const UPDATE_ENUM_VALUES_MUTATION = `
        mutation metafieldDefinitionUpdate($definition: MetafieldDefinitionUpdateInput!) {
          metafieldDefinitionUpdate(definition: $definition) {
            updatedDefinition {
              id
              validations {
                name
                value
              }
            }
            userErrors {
              field
              message
            }
          }
        }
      `;

      const variables = {
        definition: {
          namespace: metalMetafield.namespace,
          key: metalMetafield.key,
          ownerType: "PRODUCT",
          validations: [
            {
              name: "choices",
              value: JSON.stringify(updatedChoices),
            },
          ],
        },
      };

      const updateResponse = await graphqlClient(
        UPDATE_ENUM_VALUES_MUTATION,
        variables
      );
      if (updateResponse?.errors) {
        throw new Error("GraphQL mutation failed");
      }

      const userErrors =
        updateResponse?.data?.metafieldDefinitionUpdate?.userErrors;

      if (userErrors && userErrors.length > 0) {
        throw new Error("Failed to update metafield list");
      }

      /**
       *  All the products which were present in the shopify with old metal name should update with the new metal name
       */
      // Get all the products which have metafield "Metal" and "Metal is equal to oldMetalName"
      const PRODUCT_WITH_METAL_NAME_QUERY = `
        query {
          products(first: 100) {
            edges {
              node {
                id
                title
                metafields(first: 10, namespace: "custom") {
                  edges {
                    node {
                      key
                      value
                    }
                  }
                }
              }
            }
          }
        }
      `;

      const productCheckResponse = await graphqlClient(
        PRODUCT_WITH_METAL_NAME_QUERY
      );
      const productEdges = productCheckResponse?.data?.products?.edges || [];

      const productsToUpdate = productEdges.filter((edge) => {
        const metafields =
          edge.node.metafields?.edges?.map((m) => m.node) || [];
        return metafields.some(
          (mf) => mf.key === "metal" && mf.value.trim() === oldMetalName.trim()
        );
      });

      // Update the products and maintain two arrays one for successful updates and one for failed updates.
      // If we get failed updates after the update process, then revert all updated products and throw error.
      if (productsToUpdate.length > 0) {
        const successfulUpdates = [];
        const failedUpdates = [];

        for (const product of productsToUpdate) {
          const updateMutation = `
            mutation {
              metafieldsSet(metafields: [{
                namespace: "custom",
                key: "metal",
                ownerId: "${product.node.id}",
                value: "${metal_name}",
                type: "single_line_text_field"
              }]) {
                metafields {
                  id
                  key
                  value
                }
                userErrors {
                  field
                  message
                }
              }
            }
          `;

          try {
            const updateResponse = await graphqlClient(updateMutation);
            const errors =
              updateResponse?.data?.metafieldsSet?.userErrors || [];

            if (errors.length > 0) {
              console.error(
                `Failed to update product ${product.node.title}:`,
                errors
              );
              failedUpdates.push(product);
            } else {
              successfulUpdates.push(product);
            }
          } catch (err) {
            console.error(
              `Error updating product ${product.node.title}:`,
              err.message
            );
            failedUpdates.push(product);
          }
        }

        // If there are failed updates, revert successful updates and throw error
        if (failedUpdates.length > 0) {
          for (const successfulProduct of successfulUpdates) {
            const revertMutation = `
              mutation {
                metafieldsSet(metafields: [{
                  namespace: "custom",
                  key: "metal",
                  ownerId: "${successfulProduct.node.id}",
                  value: "${oldMetalName}",
                  type: "single_line_text_field"
                }]) {
                  metafields {
                    id
                    key
                    value
                  }
                  userErrors {
                    field
                    message
                  }
                }
              }
            `;

            try {
              await graphqlClient(revertMutation);
            } catch (err) {
              console.error(
                `Error reverting product ${successfulProduct.node.title}:`,
                err.message
              );
            }
          }

          const failedProductTitles = failedUpdates
            .map((p) => p.node.title)
            .join(", ");
          console.error(
            `Failed to update the following products: ${failedProductTitles}`
          );

          throw new Error(
            `Metafield predefined list got updated but Failed to update some products. Rolled back changes. Please check the logs.`
          );
        } else {
          console.log("All products updated successfully.");
        }
      }

      /**
       * Update predefined choices for the field "Metal" in the metaobject definition "Sub Collection Urls".
       */
      const GET_METAOBJECT_DEFINITIONS = `
        query {
          metaobjectDefinitions(first: 100) {
            edges {
              node {
                id
                name
                fieldDefinitions {
                  key
                  name
                  type {
                    name
                  }
                  required
                  validations {
                    name
                    value
                  }
                }
              }
            }
          }
        }
      `;

      // Fetch all metaobject definitions
      const metaobjectDefsResponse = await graphqlClient(
        GET_METAOBJECT_DEFINITIONS
      );

      const metaobjectDefs =
        metaobjectDefsResponse?.data?.metaobjectDefinitions?.edges?.map(
          (edge) => edge.node
        );

      if (!metaobjectDefs || metaobjectDefs.length === 0) {
        throw new Error("No metaobject definitions found");
      }

      // Find the "Sub Collection Urls" metaobject definition by name
      const subCollectionMetaobjectDef = metaobjectDefs.find(
        (def) => def.name === "Sub Collection Urls"
      );

      if (!subCollectionMetaobjectDef) {
        throw new Error(
          '"Sub Collection Urls" metaobject definition not found'
        );
      }

      const metalField = subCollectionMetaobjectDef.fieldDefinitions.find(
        (field) => field.key === "metal"
      );

      if (!metalField) {
        throw new Error(
          'Metal field not found in "Sub Collection Urls" metaobject'
        );
      }

      // Get the current choices
      let currentChoicesForMetalfield = [];

      const choicesValidationForMetal = metalField.validations.find(
        (v) => v.name === "choices"
      );

      if (choicesValidationForMetal && choicesValidationForMetal.value) {
        try {
          currentChoicesForMetalfield = JSON.parse(
            choicesValidationForMetal.value
          );
        } catch (e) {
          console.warn(
            "Invalid choices JSON in metal field, resetting to empty array."
          );
          currentChoicesForMetalfield = [];
        }
      }

      // Replace the old name with the new metal name in the choices array
      let updatedChoicesForMetaObjectMetalField =
        currentChoicesForMetalfield.map((choice) =>
          choice === oldMetalName ? metal_name : choice
        );

      updatedChoicesForMetaObjectMetalField = [
        ...new Set(updatedChoicesForMetaObjectMetalField),
      ];

      // Preserve other validations (excluding "choices")
      const otherValidations = metalField.validations.filter(
        (v) => v.name !== "choices"
      );

      // Add updated choices validation
      const updatedValidations = [
        ...otherValidations,
        {
          name: "choices",
          value: JSON.stringify(updatedChoicesForMetaObjectMetalField),
        },
      ];

      const UPDATE_METAOBJECT_DEFINITION = `
        mutation UpdateMetaobjectDefinition($id: ID!, $definition: MetaobjectDefinitionUpdateInput!) {
          metaobjectDefinitionUpdate(id: $id, definition: $definition) {
            metaobjectDefinition {
              id
            }
            userErrors {
              field
              message
            }
          }
        }
      `;

      const definitionInput = {
        fieldDefinitions: {
          update: {
            key: "metal",
            validations: updatedValidations,
          },
        },
      };

      const updateResponseForMetaObject = await graphqlClient(
        UPDATE_METAOBJECT_DEFINITION,
        {
          id: subCollectionMetaobjectDef.id,
          definition: definitionInput,
        }
      );

      const errors =
        updateResponseForMetaObject?.data?.metaobjectDefinitionUpdate
          ?.userErrors;

      if (errors && errors.length > 0) {
        throw new Error(`Failed to update field: ${JSON.stringify(errors)}`);
      } else {
        console.log(
          "Metal field updated successfully in the predefined choices of collection urls definition."
        );
      }

      /**
       * Find the metaobject with the old metal name and update it with the new metal name.
       */
      // Now filteredMetaobjects contains all metaobjects with oldMetalName and type "Metal"; Update the metaobjects.
      for (const metaobject of filteredMetaobjects) {
        const UPDATE_METAOBJECT_MUTATION = `
          mutation UpdateMetaobject($id: ID!, $metaobject: MetaobjectUpdateInput!) {
            metaobjectUpdate(id: $id, metaobject: $metaobject) {
              metaobject {
                id
                fields {
                  key
                  value
                }
              }
              userErrors {
                field
                message
              }
            }
          }
        `;

        const updateMetaobjectResponse = await graphqlClient(
          UPDATE_METAOBJECT_MUTATION,
          {
            id: metaobject.id,
            metaobject: {
              fields: [{ key: "name", value: metal_name }],
            },
          }
        );

        if (
          updateMetaobjectResponse?.errors &&
          updateMetaobjectResponse.errors.length > 0
        ) {
          throw new Error(
            `GraphQL Errors while updating metaobject ${
              metaobject.id
            }: ${JSON.stringify(updateMetaobjectResponse.errors)}`
          );
        }

        const userErrors =
          updateMetaobjectResponse?.data?.metaobjectUpdate?.userErrors;
        if (userErrors && userErrors.length > 0) {
          throw new Error(
            `Failed to update metaobject ${metaobject.id}: ${JSON.stringify(
              userErrors
            )}`
          );
        }

        console.log(`Metaobject ${metaobject.id} updated with new metal name.`);
      }
    }

    /**
     *  Perform the image related operations
     */
    if (files.length == 0 && existing_url === "") {
      // if the user updates without image
      existing_url = null;

      // Perform the update in the shopify metaobjects; Remove the image if it exists;
      for (const metaobject of filteredMetaobjects) {
        const UPDATE_METAOBJECT_IMAGE_MUTATION = `
          mutation UpdateMetaobject($id: ID!, $metaobject: MetaobjectUpdateInput!) {
            metaobjectUpdate(id: $id, metaobject: $metaobject) {
              metaobject {
                id
                fields {
                  key
                  value
                  type
                }
              }
              userErrors {
                field
                message
              }
            }
          }
        `;

        const updateMetaobjectResponse = await graphqlClient(
          UPDATE_METAOBJECT_IMAGE_MUTATION,
          {
            id: metaobject.id,
            metaobject: {
              fields: [{ key: "image", value: "" }],
            },
          }
        );

        if (
          updateMetaobjectResponse?.errors &&
          updateMetaobjectResponse.errors.length > 0
        ) {
          throw new Error(
            `GraphQL Errors while removing image field from metaobject ${
              metaobject.id
            }: ${JSON.stringify(updateMetaobjectResponse.errors)}`
          );
        }

        const userErrors =
          updateMetaobjectResponse?.data?.metaobjectUpdate?.userErrors;
        if (userErrors && userErrors.length > 0) {
          throw new Error(
            `Failed to remove image field from metaobject ${
              metaobject.id
            }: ${JSON.stringify(userErrors)}`
          );
        }

        console.log(`Metaobject ${metaobject.id} image field removed.`);
      }
    } else if (files.length > 0) {
      // if the user attaches the image, then update the url of the image
      files.forEach((file) => {
        const extension = file.originalname.split(".").pop();
        file.renamedTo = `${metal_name}.${extension}`;
      });

      // This api takes only one image only. So we will take the first image from the files array.
      const file = files[0];

      /**
       * Upload the file to Shopify using Staged Uploads API and get the URL.
       */
      const stagedUploadsMutation = `
          mutation generateStagedUploadTarget($input: [StagedUploadInput!]!) {
            stagedUploadsCreate(input: $input) {
              stagedTargets {
                url
                resourceUrl
                parameters {
                  name
                  value
                }
              }
              userErrors {
                field
                message
              }
            }
          }
        `;

      const uploadInput = [
        {
          filename: file.renamedTo,
          mimeType: file.mimetype,
          resource: "FILE",
          httpMethod: "POST",
        },
      ];

      // Get the upload URL and parameters from Shopify
      const stagedUploadResponse = await graphqlClient(stagedUploadsMutation, {
        input: uploadInput,
      });

      const stagedTarget =
        stagedUploadResponse?.data?.stagedUploadsCreate?.stagedTargets?.[0];

      if (!stagedTarget) {
        throw new Error("Failed to get staged upload target from Shopify");
      }

      // Prepare form-data to upload the file buffer
      const form = new FormData();
      for (const param of stagedTarget.parameters) {
        form.append(param.name, param.value);
      }
      form.append("file", file.buffer, file.renamedTo);

      // Upload the file to Shopify's storage
      await axios.post(stagedTarget.url, form, {
        headers: form.getHeaders(),
        maxBodyLength: Infinity,
      });

      file.uploadedUrl = stagedTarget.resourceUrl;

      /**
       * Create the file object using the uploaded file URL. Then use it for the metaobject creation.
       */
      const FILE_CREATE_MUTATION = `
        mutation fileCreate($files: [FileCreateInput!]!) {
          fileCreate(files: $files) {
            files {
              id
            }
            userErrors {
              field
              message
            }
          }
        }
      `;

      const filesCreateInput = [
        {
          originalSource: file.uploadedUrl, // This is the resourceUrl from staged upload
          contentType: "IMAGE",
          alt: file.originalname,
        },
      ];

      // Create the file object in Shopify and get the file ID
      const filesCreateResponse = await graphqlClient(FILE_CREATE_MUTATION, {
        files: filesCreateInput,
      });

      // Adjust response path for your mutation name
      const createdFile = filesCreateResponse?.data?.fileCreate?.files?.[0];
      const fileId = createdFile?.id;

      // Wait for 3seconds for Shopify to index the file
      await new Promise((resolve) => setTimeout(resolve, 3000));

      // Step 5: Get permanent URL using ID-based file query
      const FILE_QUERY = `
        query getFile($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on MediaImage {
              id
              image {
                url
              }
            }
            ... on GenericFile {
              id
              url
            }
          }
        }
      `;

      const fileQueryResponse = await graphqlClient(FILE_QUERY, {
        ids: [fileId],
      });

      const fileNode = fileQueryResponse?.data?.nodes?.[0];

      let permanentUrl = null;

      if (fileNode?.image?.url) {
        permanentUrl = fileNode.image.url;
      } else if (fileNode?.url) {
        permanentUrl = fileNode.url;
      }

      if (!permanentUrl) {
        throw new Error(
          "Failed to fetch permanent URL for uploaded file from Shopify"
        );
      }

      file.uploadedUrl = permanentUrl;
      existing_url = permanentUrl;

      /**
       * Perform the update in the "image" field of shopify metaobject;
       */
      for (const metaobject of filteredMetaobjects) {
        const UPDATE_METAOBJECT_IMAGE_MUTATION = `
          mutation UpdateMetaobject($id: ID!, $metaobject: MetaobjectUpdateInput!) {
            metaobjectUpdate(id: $id, metaobject: $metaobject) {
              metaobject {
                id
                fields {
                  key
                  value
                  type
                }
              }
              userErrors {
                field
                message
              }
            }
          }
        `;

        const updateMetaobjectResponse = await graphqlClient(
          UPDATE_METAOBJECT_IMAGE_MUTATION,
          {
            id: metaobject.id,
            metaobject: {
              fields: [{ key: "image", value: fileId }],
            },
          }
        );

        if (
          updateMetaobjectResponse?.errors &&
          updateMetaobjectResponse.errors.length > 0
        ) {
          throw new Error(
            `GraphQL Errors while updating image field from metaobject ${
              metaobject.id
            }: ${JSON.stringify(updateMetaobjectResponse.errors)}`
          );
        }

        const userErrors =
          updateMetaobjectResponse?.data?.metaobjectUpdate?.userErrors;
        if (userErrors && userErrors.length > 0) {
          throw new Error(
            `Failed to update image field from metaobject ${
              metaobject.id
            }: ${JSON.stringify(userErrors)}`
          );
        }

        console.log(`Metaobject ${metaobject.id} image field was updated.`);
      }
    }

    /**
     *  Perform the final update in the stlr_metals table in the DB
     */
    const slug = metal_name.toLowerCase();

    await connection.query(
      "UPDATE stlr_metals SET name = ?, slug = ?, image = ? WHERE metal_id = ?",
      [metal_name, slug, existing_url, numericId]
    );

    await connection.commit();

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Metal updated successfully.",
    });
  } catch (error) {
    await connection.rollback();
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  } finally {
    connection.release();
  }
};

const deleteSingleMetal = async (req, res) => {
  const deleteMetalSchema = Joi.object({
    id: Joi.number().integer().positive().required().messages({
      "number.base": "Metal ID must be a number.",
      "number.integer": "Metal ID must be an integer.",
      "number.positive": "Metal ID must be a positive number.",
      "any.required": "Metal ID is required.",
    }),
  });

  const { id } = req.params;
  const numericId = Number(id);

  const { error: idError } = deleteMetalSchema.validate({ id: numericId });

  if (idError) {
    return res.status(400).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: idError.details[0].message,
    });
  }

  const connection = await sqldb.getConnection();

  try {
    await connection.beginTransaction();

    // Check if metal exists
    const [metals] = await connection.query(
      "SELECT * FROM stlr_metals WHERE metal_id = ? LIMIT 1",
      [numericId]
    );

    if (metals.length === 0) {
      await connection.rollback();
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Metal not found. Please provide a valid metal ID.",
      });
    }

    const metalName = metals[0].name;

    /**
     *  Check whether any product has this metal name or not. If any product has this metal name, then return error.
     */
    const PRODUCT_WITH_METAL_NAME_QUERY = `
      query {
        products(first: 100) {
          edges {
            node {
              id
              title
              metafields(first: 10, namespace: "custom") {
                edges {
                  node {
                    key
                    value
                  }
                }
              }
            }
          }
        }
      }
    `;

    const productCheckResponse = await graphqlClient(
      PRODUCT_WITH_METAL_NAME_QUERY
    );
    const productEdges = productCheckResponse?.data?.products?.edges || [];

    const usedProduct = productEdges.find((edge) => {
      const metafields = edge.node.metafields?.edges?.map((m) => m.node) || [];
      return metafields.some(
        (mf) => mf.key === "metal" && mf.value.trim() === metalName.trim()
      );
    });

    if (usedProduct) {
      await connection.rollback();
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: `Cannot delete metal '${metalName}' because it is used in one or more products.`,
      });
    }

    // Check if metal is used in local variations table
    const [associatedVariations] = await connection.query(
      `SELECT COUNT(*) as count FROM stlr_ring_variations WHERE metal_id = ?`,
      [numericId]
    );

    if (associatedVariations[0].count > 0) {
      await connection.rollback();
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: `Cannot delete metal '${metalName}' because it is used in one or more variations.`,
      });
    }

    /**
     *  If there is no product with this metal name, then delete the metal name from the predefined metafield validations.
     */
    // Get the Metal Name metafield id
    const METAFIELD_DEFINITIONS_QUERY = `
      query {
        metafieldDefinitions(first: 100, ownerType: PRODUCT) {
          edges {
            node {
              id
              name
              namespace
              key
              validations {
                name
                value
              }
            }
          }
        }
      }
    `;

    const response1 = await graphqlClient(METAFIELD_DEFINITIONS_QUERY);
    const definitions = response1?.data?.metafieldDefinitions?.edges?.map(
      (edge) => edge.node
    );
    const metalNameMetafield = definitions.find((def) => def.name === "Metal");

    if (!metalNameMetafield) {
      throw new Error("Metal metafield definition not found");
    }

    const metalNameMetafieldId = metalNameMetafield.id;

    // Get the existing metal names from the metafield "validations"
    const METAFIELD_PREDEFINED_VALUES_QUERY = `
      query {
        metafieldDefinition(id: "${metalNameMetafieldId}") {
          id
          name
          namespace
          key
          validations {
            name
            value
          }
        }
      }
    `;

    const response2 = await graphqlClient(METAFIELD_PREDEFINED_VALUES_QUERY);
    const definition =
      response2?.data?.metafieldDefinition ?? response2?.metafieldDefinition;
    const validations = definition?.validations || [];

    const choicesValidation = validations.find((v) => v.name === "choices");
    let currentChoices = [];

    if (choicesValidation && choicesValidation.value) {
      try {
        currentChoices = JSON.parse(choicesValidation.value);
      } catch (e) {
        currentChoices = [];
      }
    }

    // Remove the deleted metal name from the choices array
    const updatedChoices = currentChoices.filter(
      (choice) => choice !== metalName
    );

    // Update the metafield definition with the new list of metal names
    const UPDATE_ENUM_VALUES_MUTATION = `
      mutation metafieldDefinitionUpdate($definition: MetafieldDefinitionUpdateInput!) {
        metafieldDefinitionUpdate(definition: $definition) {
          updatedDefinition {
            id
            validations {
              name
              value
            }
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const variables = {
      definition: {
        namespace: metalNameMetafield.namespace,
        key: metalNameMetafield.key,
        ownerType: "PRODUCT",
        validations: [
          {
            name: "choices",
            value: JSON.stringify(updatedChoices),
          },
        ],
      },
    };

    const updateResponse = await graphqlClient(
      UPDATE_ENUM_VALUES_MUTATION,
      variables
    );

    if (updateResponse?.errors) {
      throw new Error("GraphQL mutation failed");
    }

    const userErrors =
      updateResponse?.data?.metafieldDefinitionUpdate?.userErrors;

    if (userErrors && userErrors.length > 0) {
      throw new Error("Failed to update Shopify metal list");
    }

    /**
     * Delete the predefined choices for the field "Metal" in the metaobject definition "Sub Collection Urls".
     */
    const GET_METAOBJECT_DEFINITIONS = `
      query {
        metaobjectDefinitions(first: 100) {
          edges {
            node {
              id
              name
              fieldDefinitions {
                key
                name
                type {
                  name
                }
                required
                validations {
                  name
                  value
                }
              }
            }
          }
        }
      }
    `;

    // Fetch all metaobject definitions
    const metaobjectDefsResponse = await graphqlClient(
      GET_METAOBJECT_DEFINITIONS
    );

    const metaobjectDefs =
      metaobjectDefsResponse?.data?.metaobjectDefinitions?.edges?.map(
        (edge) => edge.node
      );

    if (!metaobjectDefs || metaobjectDefs.length === 0) {
      throw new Error("No metaobject definitions found");
    }

    // Find the "Sub Collection Urls" metaobject definition by name
    const subCollectionMetaobjectDef = metaobjectDefs.find(
      (def) => def.name === "Sub Collection Urls"
    );

    if (!subCollectionMetaobjectDef) {
      throw new Error('"Sub Collection Urls" metaobject definition not found');
    }

    const metalField = subCollectionMetaobjectDef.fieldDefinitions.find(
      (field) => field.key === "metal"
    );

    if (!metalField) {
      throw new Error(
        'Metal field not found in "Sub Collection Urls" metaobject'
      );
    }

    let currentChoicesForMetalfield = [];

    const choicesValidationForMetal = metalField.validations.find(
      (v) => v.name === "choices"
    );

    if (choicesValidationForMetal && choicesValidationForMetal.value) {
      try {
        currentChoicesForMetalfield = JSON.parse(
          choicesValidationForMetal.value
        );
      } catch (e) {
        currentChoicesForMetalfield = [];
      }
    }

    // Remove the metal_name from the choices array
    currentChoicesForMetalfield = currentChoicesForMetalfield.filter(
      (choice) => choice !== metalName
    );

    // Preserve other validations (excluding "choices")
    const otherValidations = metalField.validations.filter(
      (v) => v.name !== "choices"
    );

    // Add updated choices validation
    const updatedValidations = [
      ...otherValidations,
      {
        name: "choices",
        value: JSON.stringify(currentChoicesForMetalfield),
      },
    ];

    const UPDATE_METAOBJECT_DEFINITION = `
      mutation UpdateMetaobjectDefinition($id: ID!, $definition: MetaobjectDefinitionUpdateInput!) {
        metaobjectDefinitionUpdate(id: $id, definition: $definition) {
          metaobjectDefinition {
            id
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const definitionInput = {
      fieldDefinitions: {
        update: {
          key: "metal",
          validations: updatedValidations,
        },
      },
    };

    const updateResponseForMetaObject = await graphqlClient(
      UPDATE_METAOBJECT_DEFINITION,
      {
        id: subCollectionMetaobjectDef.id,
        definition: definitionInput,
      }
    );

    const errors =
      updateResponseForMetaObject?.data?.metaobjectDefinitionUpdate?.userErrors;

    if (errors && errors.length > 0) {
      throw new Error(`Failed to update field: ${JSON.stringify(errors)}`);
    } else {
      console.log("Metal field updated successfully.");
    }

    /**
     * Delete the metaobject with this metal name in the definition "filter_images".
     */
    // Get all metaobjects with the old metal name
    const METAOBJECTS_BY_TYPE_QUERY = `
        query MetaobjectsByType($type: String!) {
          metaobjects(first: 100, type: $type) {
            edges {
              node {
                id
                type
                fields {
                  key
                  value
                }
              }
            }
          }
        }
    `;

    const metaobjectsResponse = await graphqlClient(METAOBJECTS_BY_TYPE_QUERY, {
      type: "filter_images",
    });

    const metaobjects =
      metaobjectsResponse?.data?.metaobjects?.edges?.map((edge) => edge.node) ||
      [];

    // Filter metaobjects whose "name" field matches oldMetalName and "type" field is "Metal"
    const filteredMetaobjects = metaobjects.filter((metaobject) => {
      if (!Array.isArray(metaobject.fields)) return false;
      const nameField = metaobject.fields.find((f) => f.key === "name");
      const typeField = metaobject.fields.find((f) => f.key === "type");
      return (
        nameField &&
        typeof nameField.value === "string" &&
        nameField.value.trim() === metalName.trim() &&
        typeField &&
        typeof typeField.value === "string" &&
        typeField.value.trim() === "Metal"
      );
    });

    // Perform the deletion of the metaobjects if there are any filtered results
    if (filteredMetaobjects.length > 0) {
      const DELETE_METAOBJECT_MUTATION = `
        mutation DeleteMetaobject($id: ID!) {
          metaobjectDelete(id: $id) {
            deletedId
            userErrors {
              field
              message
            }
          }
        }
      `;

      for (const metaobject of filteredMetaobjects) {
        const deleteResponse = await graphqlClient(DELETE_METAOBJECT_MUTATION, {
          id: metaobject.id,
        });

        if (
          deleteResponse?.errors &&
          deleteResponse.errors.length > 0 &&
          deleteResponse.errors[0].message !== "Metaobject not found"
        ) {
          throw new Error(
            `Failed to delete metaobject ${metaobject.id}: ${JSON.stringify(
              deleteResponse.errors
            )}`
          );
        }

        const userErrors = deleteResponse?.data?.metaobjectDelete?.userErrors;
        if (userErrors && userErrors.length > 0) {
          throw new Error(
            `Failed to delete metaobject ${metaobject.id}: ${JSON.stringify(
              userErrors
            )}`
          );
        }

        console.log(`Metaobject ${metaobject.id} deleted successfully.`);
      }
    }

    /**
     *  Update the table stlr_metals.
     */
    await connection.execute(
      `DELETE FROM wedding_band.stlr_metals WHERE metal_id = ?`,
      [numericId]
    );

    await connection.commit();

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Metal deleted successfully.",
    });
  } catch (error) {
    await connection.rollback();
    console.error("Delete metal error:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
};

export {
  createMetal,
  getMetal,
  getMetalsByIds,
  updateSingleMetal,
  deleteSingleMetal,
};
