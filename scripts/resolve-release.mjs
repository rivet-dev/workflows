import { appendFile } from "node:fs/promises";

const args = Object.fromEntries(
	process.argv.slice(2).map((argument) => {
		const [key, ...value] = argument.replace(/^--/, "").split("=");
		return [key, value.join("=")];
	}),
);

const version = args.version;
if (!version || !/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/.test(version)) {
	throw new Error(`invalid release version: ${version ?? "<empty>"}`);
}

const sanitize = (value) =>
	value
		.toLowerCase()
		.replace(/[^a-z0-9._-]+/g, "-")
		.replace(/^[._-]+|[._-]+$/g, "")
		.slice(0, 64);

let tag = args.tag ?? "auto";
if (tag === "auto") {
	const prerelease = version.split("-")[1]?.split(".")[0];
	tag = prerelease === "rc" || prerelease === "next" ? prerelease : "latest";
} else if (tag === "preview") {
	tag = `preview-${sanitize(args.branch ?? "branch")}`;
}
if (!/^[a-z0-9][a-z0-9._-]{0,127}$/.test(tag)) {
	throw new Error(`invalid npm dist-tag: ${tag}`);
}

const output = [
	`version=${version}`,
	`npm_tag=${tag}`,
	`real_release=${!tag.startsWith("preview-")}`,
];
if (process.env.GITHUB_OUTPUT) {
	await appendFile(process.env.GITHUB_OUTPUT, `${output.join("\n")}\n`);
} else {
	console.log(output.join("\n"));
}
