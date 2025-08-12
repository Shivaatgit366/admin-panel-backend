import { httpStatusCodes } from "./errorController.js";
import sqldb from "../mysqldb.js";
import { graphqlClient } from "../helpers/commonFnHelper.js";
import Joi from "joi";
import axios from "axios";
import FormData from "form-data";

const getShape = async (req, res) => {
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

    // Fetch all shapes with sorting
    const query = `
      SELECT 
        s.*
      FROM stlr_shapes s
      ORDER BY s.shape_name ${sort};
    `;

    const [shapes] = await sqldb.query(query);

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Shapes fetched successfully.",
      result: shapes,
    });
  } catch (error) {
    console.error("Error fetching shapes:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
};

const getShapesByIds = async (req, res) => {
  const schema = Joi.object({
    shape_ids: Joi.array()
      .items(Joi.number().integer().positive().required())
      .min(1)
      .required()
      .messages({
        "array.base": "shape_ids must be an array.",
        "array.min": "At least one shape_id must be provided.",
        "any.required": "shape_ids array is required.",
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

  const { shape_ids } = req.body;

  const connection = await sqldb.getConnection();
  try {
    const [shapes] = await connection.execute(
      `SELECT * FROM wedding_band.stlr_shapes 
       WHERE shape_id IN (${shape_ids.map(() => "?").join(", ")})`,
      shape_ids
    );

    return res.status(200).json({
      status: 200,
      success: true,
      message: "Shapes fetched successfully.",
      result: shapes,
    });
  } catch (error) {
    console.error("Error fetching shapes:", error);
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal Server Error",
    });
  } finally {
    connection.release();
  }
};

const createShape = async (req, res) => {
  // Joi schema for shape_name validation
  const shapeSchema = Joi.object({
    shape_name: Joi.string()
      .trim()
      .min(2)
      .max(100)
      .regex(/^(?!\d+$).*$/) // Prevents purely numeric names
      .required()
      .messages({
        "string.empty": "Shape name is required.",
        "string.min": "Shape name must be at least 2 characters long.",
        "string.max": "Shape name cannot exceed 100 characters.",
        "string.pattern.base": "Shape name cannot be only numbers.",
      }),
  });

  const { error } = shapeSchema.validate(req.body);
  if (error) {
    return res.status(400).json({
      status: httpStatusCodes.NOT_FOUND,
      success: false,
      message: error.details[0].message,
    });
  }

  const { shape_name } = req.body;
  const files = req.files || [];

  if (!shape_name) {
    return res.status(400).json({
      status: httpStatusCodes.NOT_FOUND,
      success: false,
      message: "Shape name is required",
    });
  }

  const connection = await sqldb.getConnection();

  try {
    await connection.beginTransaction();

    // Check if shape already exists
    const [existingShape] = await connection.execute(
      `SELECT shape_id FROM wedding_band.stlr_shapes WHERE shape_name = ?`,
      [shape_name]
    );

    if (existingShape.length > 0) {
      return res.status(409).json({
        status: 409,
        success: false,
        message: "Shape name already exists",
      });
    }

    /**
     * Update the predefined values present in the metafield definition called "Shape".
     */
    const SHAPE_METAFIELD_DEFINITIONS_QUERY = `
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

    const response1 = await graphqlClient(SHAPE_METAFIELD_DEFINITIONS_QUERY);
    const definitions = response1?.data?.metafieldDefinitions?.edges?.map(
      (edge) => edge.node
    );

    const shapeMetafield = definitions.find((def) => def.name === "Shape");
    if (!shapeMetafield) {
      throw new Error("Shape metafield definition not found");
    }

    const shapeMetafieldId = shapeMetafield.id;

    const SHAPE_METAFIELD_PREDEFINED_VALUES_QUERY = `
      query {
        metafieldDefinition(id: "${shapeMetafieldId}") {
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

    const response2 = await graphqlClient(
      SHAPE_METAFIELD_PREDEFINED_VALUES_QUERY
    );
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

    if (!currentChoices.includes(shape_name)) {
      currentChoices.push(shape_name);
    }

    const UPDATE_SHAPE_ENUM_VALUES_MUTATION = `
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
        namespace: shapeMetafield.namespace,
        key: shapeMetafield.key,
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
      UPDATE_SHAPE_ENUM_VALUES_MUTATION,
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
      throw new Error("Failed to update Shopify shape list");
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

    const shapeField = subCollectionMetaobjectDef.fieldDefinitions.find(
      (field) => field.key === "shape"
    );

    if (!shapeField) {
      throw new Error(
        'Shape field not found in "Sub Collection Urls" metaobject'
      );
    }

    let currentChoicesForShapefield = [];

    const choicesValidationForShape = shapeField.validations.find(
      (v) => v.name === "choices"
    );

    if (choicesValidationForShape && choicesValidationForShape.value) {
      try {
        currentChoicesForShapefield = JSON.parse(
          choicesValidationForShape.value
        );
      } catch (e) {
        console.warn(
          "Invalid choices JSON in shape field, resetting to empty array."
        );
        currentChoicesForShapefield = [];
      }
    }

    // Add new shape_name if not already included
    if (!currentChoicesForShapefield.includes(shape_name)) {
      currentChoicesForShapefield.push(shape_name);
    }

    // Preserve other validations (excluding "choices")
    const otherValidations = shapeField.validations.filter(
      (v) => v.name !== "choices"
    );

    // Add updated choices validation
    const updatedValidations = [
      ...otherValidations,
      {
        name: "choices",
        value: JSON.stringify(currentChoicesForShapefield),
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
          key: "shape",
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
      console.log("Shape field updated successfully.");
    }

    /**
     * If there is no file uploaded, then create a metaobject with the shape name only.
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
          { key: "name", value: shape_name },
          { key: "type", value: "Shape" },
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
       * If the file is uploaded, then upload the file to the Shopify and create a metaobject with the shape name and image url.
       */
      files.forEach((file) => {
        const extension = file.originalname.split(".").pop();
        file.renamedTo = `${shape_name}.${extension}`;
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
       * Create the metaobject with the shape name and image url.
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
          { key: "name", value: shape_name },
          { key: "type", value: "Shape" },
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
    const [result] = await connection.execute(
      `INSERT INTO wedding_band.stlr_shapes (shape_name, image) VALUES (?, ?)`,
      [shape_name, files.length > 0 ? files[0].uploadedUrl : null]
    );

    await connection.commit();

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Shape created successfully",
      result: result.insertId,
    });
  } catch (error) {
    console.error("Error creating shape:", error);
    return res.status(500).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: "Internal Server Error",
    });
  } finally {
    connection.release();
  }
};

const updateSingleShape = async (req, res) => {
  const updateShapeSchema = Joi.object({
    id: Joi.number().integer().positive().required().messages({
      "number.base": "Shape ID must be a number.",
      "number.integer": "Shape ID must be an integer.",
      "number.positive": "Shape ID must be a positive number.",
      "any.required": "Shape ID is required.",
    }),
  });

  const shapeNameSchema = Joi.object({
    shape_name: Joi.string()
      .trim()
      .min(2)
      .max(100)
      .regex(/^(?!\d+$).*$/)
      .required()
      .messages({
        "string.empty": "Shape name is required.",
        "string.min": "Shape name must be at least 2 characters long.",
        "string.max": "Shape name cannot exceed 100 characters.",
        "string.pattern.base": "Shape name cannot be only numbers.",
      }),
    existing_url: Joi.string().allow("").required().messages({
      "string.base": "Existing URL must be a string.",
      "any.required": "Existing URL is required.",
    }),
  });

  const { id } = req.params;
  const numericId = Number(id);

  const { error: idError } = updateShapeSchema.validate({ id: numericId });
  if (idError) {
    return res.status(400).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: idError.details[0].message,
    });
  }

  const { error: nameError } = shapeNameSchema.validate(req.body);
  if (nameError) {
    return res.status(400).json({
      status: httpStatusCodes.BAD_REQUEST,
      success: false,
      message: nameError.details[0].message,
    });
  }

  const { shape_name } = req.body;
  const files = req.files || [];
  let existing_url = req.body.existing_url;

  const connection = await sqldb.getConnection();

  try {
    await connection.beginTransaction();

    // Check if shape exists
    const [shapes] = await connection.query(
      "SELECT * FROM stlr_shapes WHERE shape_id = ? LIMIT 1",
      [numericId]
    );

    if (shapes.length === 0) {
      await connection.rollback();
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Shape not found. Please provide a valid shape ID.",
      });
    }

    const oldShapeName = shapes[0].shape_name;
    const oldImageUrl = shapes[0].image;

    // Check for duplicate shape name
    const [existingShape] = await connection.query(
      "SELECT shape_id FROM stlr_shapes WHERE shape_name = ? AND shape_id != ? LIMIT 1",
      [shape_name, numericId]
    );

    if (existingShape.length > 0) {
      await connection.rollback();
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: "Shape name already exists. Please choose a different name.",
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

    // Filter metaobjects whose "name" field matches oldShapeName and "type" field is "Shape"
    const filteredMetaobjects = metaobjects.filter((metaobject) => {
      if (!Array.isArray(metaobject.fields)) return false;
      const nameField = metaobject.fields.find((f) => f.key === "name");
      const typeField = metaobject.fields.find((f) => f.key === "type");
      return (
        nameField &&
        typeof nameField.value === "string" &&
        nameField.value.trim() === oldShapeName.trim() &&
        typeField &&
        typeof typeField.value === "string" &&
        typeField.value.trim() === "Shape"
      );
    });

    // If no metaobjects found, it means manually someone has deleted the metaobject. We will throw the error.
    if (filteredMetaobjects.length === 0) {
      await connection.rollback();
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: `You tried to update ${oldShapeName}. No metaobject found in the shopify with ${oldShapeName}. Can not be updated. Data mismatch. Please contact support.`,
      });
    }

    /**
     * Perform the shopify operations only if the new shape name and old shape name are different
     */
    if (oldShapeName.trim() !== shape_name.trim()) {
      /**
       * Update the shape name in the predefined shape names list.
       */
      // Get the Shape Name metafield id
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
      const shapeMetafield = definitions.find((def) => def.name === "Shape");

      if (!shapeMetafield) {
        throw new Error("Shape metafield definition not found");
      }

      const shapeMetafieldId = shapeMetafield.id;

      // Get the existing shape names from the metafield "validations"
      const METAFIELD_PREDEFINED_VALUES_QUERY = `
        query {
          metafieldDefinition(id: "${shapeMetafieldId}") {
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

      // Replace the old name with the new shape name in the choices array
      let updatedChoices = currentChoices.map((choice) =>
        choice === oldShapeName ? shape_name : choice
      );

      updatedChoices = [...new Set(updatedChoices)];

      // Update the metafield definition with the new list of shape names
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
          namespace: shapeMetafield.namespace,
          key: shapeMetafield.key,
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
       *  All the products which were present in the shopify with old shape name should update with the new shape name
       */
      // Get all the products which have metafield "Shape" and "Shape is equal to oldShapeName"
      const PRODUCT_WITH_SHAPE_NAME_QUERY = `
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
        PRODUCT_WITH_SHAPE_NAME_QUERY
      );

      const productEdges = productCheckResponse?.data?.products?.edges || [];

      const productsToUpdate = productEdges.filter((edge) => {
        const metafields =
          edge.node.metafields?.edges?.map((m) => m.node) || [];
        return metafields.some(
          (mf) => mf.key === "shape" && mf.value.trim() === oldShapeName.trim()
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
                key: "shape",
                ownerId: "${product.node.id}",
                value: "${shape_name}",
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
              failedUpdates.push(product);
            } else {
              successfulUpdates.push(product);
            }
          } catch (err) {
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
                  key: "shape",
                  ownerId: "${successfulProduct.node.id}",
                  value: "${oldShapeName}",
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
          throw new Error(
            `Metafield predefined list got updated but Failed to update some products: ${failedProductTitles}. Rolled back changes.`
          );
        } else {
          console.log("All products updated successfully.");
        }
      }

      /**
       * Update predefined choices for the field "Shape" in the metaobject definition "Sub Collection Urls".
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

      const shapeField = subCollectionMetaobjectDef.fieldDefinitions.find(
        (field) => field.key === "shape"
      );

      if (!shapeField) {
        throw new Error(
          'Shape field not found in "Sub Collection Urls" metaobject'
        );
      }

      // Get the current choices
      let currentChoicesForShapefield = [];

      const choicesValidationForShape = shapeField.validations.find(
        (v) => v.name === "choices"
      );

      if (choicesValidationForShape && choicesValidationForShape.value) {
        try {
          currentChoicesForShapefield = JSON.parse(
            choicesValidationForShape.value
          );
        } catch (e) {
          console.warn(
            "Invalid choices JSON in style field, resetting to empty array."
          );
          currentChoicesForShapefield = [];
        }
      }

      // Replace the old name with the new shape name in the choices array
      let updatedChoicesForMetaObjectShapeField =
        currentChoicesForShapefield.map((choice) =>
          choice === oldShapeName ? shape_name : choice
        );

      updatedChoicesForMetaObjectShapeField = [
        ...new Set(updatedChoicesForMetaObjectShapeField),
      ];

      // Preserve other validations (excluding "choices")
      const otherValidations = shapeField.validations.filter(
        (v) => v.name !== "choices"
      );

      // Add updated choices validation
      const updatedValidations = [
        ...otherValidations,
        {
          name: "choices",
          value: JSON.stringify(updatedChoicesForMetaObjectShapeField),
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
            key: "shape",
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
          "Shape field updated successfully in the predefined choices of collection urls definition."
        );
      }

      /**
       * Find the metaobject with the old shape name and update it with the new shape name.
       */
      // Now filteredMetaobjects contains all metaobjects with oldShapeName and type "Shape"; Update the metaobjects.
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
              fields: [{ key: "name", value: shape_name }],
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

        console.log(`Metaobject ${metaobject.id} updated with new shape name.`);
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
        file.renamedTo = `${shape_name}.${extension}`;
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
     *  Perform the final update in the stlr_shapes table in the DB
     */
    await connection.query(
      "UPDATE stlr_shapes SET shape_name = ?, image = ? WHERE shape_id = ?",
      [shape_name, existing_url, numericId]
    );

    await connection.commit();

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Shape updated successfully.",
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

const deleteSingleShape = async (req, res) => {
  const deleteShapeSchema = Joi.object({
    id: Joi.number().integer().positive().required().messages({
      "number.base": "Shape ID must be a number.",
      "number.integer": "Shape ID must be an integer.",
      "number.positive": "Shape ID must be a positive number.",
      "any.required": "Shape ID is required.",
    }),
  });

  const { id } = req.params;
  const numericId = Number(id);

  const { error: idError } = deleteShapeSchema.validate({ id: numericId });

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

    // Check if shape exists
    const [shapes] = await connection.query(
      "SELECT * FROM stlr_shapes WHERE shape_id = ? LIMIT 1",
      [numericId]
    );

    if (shapes.length === 0) {
      await connection.rollback();
      return res.status(404).json({
        status: httpStatusCodes.NOT_FOUND,
        success: false,
        message: "Shape not found. Please provide a valid shape ID.",
      });
    }

    const shapeName = shapes[0].shape_name;

    /**
     *  Check whether any product has this shape name or not. If any product has this shape name, then return error.
     */
    const PRODUCT_WITH_SHAPE_NAME_QUERY = `
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
      PRODUCT_WITH_SHAPE_NAME_QUERY
    );
    const productEdges = productCheckResponse?.data?.products?.edges || [];

    const usedProduct = productEdges.find((edge) => {
      const metafields = edge.node.metafields?.edges?.map((m) => m.node) || [];
      return metafields.some(
        (mf) => mf.key === "shape" && mf.value.trim() === shapeName.trim()
      );
    });

    if (usedProduct) {
      await connection.rollback();
      return res.status(400).json({
        status: httpStatusCodes.BAD_REQUEST,
        success: false,
        message: `Cannot delete shape '${shapeName}' because it is used in one or more products.`,
      });
    }

    /**
     *  If there is no product with this shape name, then delete the shape name from the predefined metafield validations.
     */
    // Get the Shape Name metafield definition
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
    const shapeNameMetafield = definitions.find((def) => def.name === "Shape");

    if (!shapeNameMetafield) {
      throw new Error("Shape Name metafield definition not found");
    }

    const shapeNameMetafieldId = shapeNameMetafield.id;

    // Get the existing shape names from the metafield "validations"
    const METAFIELD_PREDEFINED_VALUES_QUERY = `
      query {
        metafieldDefinition(id: "${shapeNameMetafieldId}") {
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

    // Remove the deleted shape name from the choices array
    const updatedChoices = currentChoices.filter(
      (choice) => choice !== shapeName
    );

    // Update the metafield definition with the new list of shape names
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
        namespace: shapeNameMetafield.namespace,
        key: shapeNameMetafield.key,
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
      throw new Error("Failed to update Shopify shape list");
    }

    /**
     * Delete the predefined choices for the field "Shape" in the metaobject definition "Sub Collection Urls".
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

    const shapeField = subCollectionMetaobjectDef.fieldDefinitions.find(
      (field) => field.key === "shape"
    );

    if (!shapeField) {
      throw new Error(
        'Shape field not found in "Sub Collection Urls" metaobject'
      );
    }

    let currentChoicesForShapefield = [];

    const choicesValidationForShape = shapeField.validations.find(
      (v) => v.name === "choices"
    );

    if (choicesValidationForShape && choicesValidationForShape.value) {
      try {
        currentChoicesForShapefield = JSON.parse(
          choicesValidationForShape.value
        );
      } catch (e) {
        currentChoicesForShapefield = [];
      }
    }

    // Remove the shape_name from the choices array
    currentChoicesForShapefield = currentChoicesForShapefield.filter(
      (choice) => choice !== shapeName
    );

    // Preserve other validations (excluding "choices")
    const otherValidations = shapeField.validations.filter(
      (v) => v.name !== "choices"
    );

    // Add updated choices validation
    const updatedValidations = [
      ...otherValidations,
      {
        name: "choices",
        value: JSON.stringify(currentChoicesForShapefield),
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
          key: "shape",
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
      console.log("Shape field updated successfully.");
    }

    /**
     * Delete the metaobject with this shape name in the definition "filter_images".
     */
    // Get all metaobjects with the old shape name
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

    // Filter metaobjects whose "name" field matches oldShapeName and "type" field is "Shape"
    const filteredMetaobjects = metaobjects.filter((metaobject) => {
      if (!Array.isArray(metaobject.fields)) return false;
      const nameField = metaobject.fields.find((f) => f.key === "name");
      const typeField = metaobject.fields.find((f) => f.key === "type");
      return (
        nameField &&
        typeof nameField.value === "string" &&
        nameField.value.trim() === shapeName.trim() &&
        typeField &&
        typeof typeField.value === "string" &&
        typeField.value.trim() === "Shape"
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
     *  Update the stlr_shapes
     */
    await connection.execute(
      `DELETE FROM wedding_band.stlr_shapes WHERE shape_id = ?`,
      [numericId]
    );

    await connection.commit();

    return res.status(200).json({
      status: httpStatusCodes.OK,
      success: true,
      message: "Shape deleted successfully.",
    });
  } catch (error) {
    await connection.rollback();
    console.error("Delete shape error:", error);
    return res.status(500).json({
      status: httpStatusCodes.INTERNAL_SERVER,
      success: false,
      message: "Internal Server Error",
    });
  }
};

export {
  getShape,
  createShape,
  getShapesByIds,
  updateSingleShape,
  deleteSingleShape,
};
