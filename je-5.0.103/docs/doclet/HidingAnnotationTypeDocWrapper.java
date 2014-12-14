/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.AnnotationTypeDoc;
import com.sun.javadoc.AnnotationTypeElementDoc;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.ConstructorDoc;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.MethodDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.ParamTag;
import com.sun.javadoc.ParameterizedType;
import com.sun.javadoc.SeeTag;
import com.sun.javadoc.SourcePosition;
import com.sun.javadoc.Tag;
import com.sun.javadoc.Type;
import com.sun.javadoc.TypeVariable;
import com.sun.javadoc.WildcardType;

class HidingAnnotationTypeDocWrapper extends HidingWrapper
                                     implements AnnotationTypeDoc {
    public HidingAnnotationTypeDocWrapper(AnnotationTypeDoc type,
                                          Map mapWrappers) {
        super(type, mapWrappers);
    }

    private AnnotationTypeDoc _getAnnotationTypeDoc() {
        return (AnnotationTypeDoc)getWrappedObject();
    }

    /**
     * @deprecated
     */
    public PackageDoc[] importedPackages() {
        return (PackageDoc[])
                wrapOrHide(_getAnnotationTypeDoc().importedPackages());
    }

    /**
     * @deprecated
     */
    public ClassDoc[] importedClasses() {
        return (ClassDoc[])
                wrapOrHide(_getAnnotationTypeDoc().importedClasses());
    }

    public ClassDoc findClass(String className) {
        return (ClassDoc)
                wrapOrHide(_getAnnotationTypeDoc().findClass(className));
    }

    public ClassDoc[] innerClasses() {
        return (ClassDoc[])wrapOrHide(_getAnnotationTypeDoc().innerClasses());
    }

    public ClassDoc[] innerClasses(boolean filter) {
        return (ClassDoc[])
                wrapOrHide(_getAnnotationTypeDoc().innerClasses(filter));
    }

    public ConstructorDoc[] constructors() {
        return (ConstructorDoc[])
                wrapOrHide(_getAnnotationTypeDoc().constructors());
    }

    public ConstructorDoc[] constructors(boolean filter) {
        return (ConstructorDoc[])
                wrapOrHide(_getAnnotationTypeDoc().constructors(filter));
    }

    public MethodDoc[] methods() {
        return (MethodDoc[])wrapOrHide(_getAnnotationTypeDoc().methods());
    }

    public MethodDoc[] methods(boolean filter) {
        return (MethodDoc[])
                wrapOrHide(_getAnnotationTypeDoc().methods(filter));
    }

    public FieldDoc[] enumConstants() {
        return (FieldDoc[])
                wrapOrHide(_getAnnotationTypeDoc().enumConstants());
    }

    public FieldDoc[] fields() {
        return (FieldDoc[])wrapOrHide(_getAnnotationTypeDoc().fields());
    }

    public FieldDoc[] fields(boolean filter) {
        return (FieldDoc[])wrapOrHide(_getAnnotationTypeDoc().fields(filter));
    }

    public ParamTag[] typeParamTags() {
        return (ParamTag[])wrapOrHide(_getAnnotationTypeDoc().typeParamTags());
    }

    public TypeVariable[] typeParameters() {
        return (TypeVariable[])
                wrapOrHide(_getAnnotationTypeDoc().typeParameters());
    }

    public Type[] interfaceTypes() {
        return (Type[])wrapOrHide(_getAnnotationTypeDoc().interfaceTypes());
    }

    public ClassDoc[] interfaces() {
        return (ClassDoc[])wrapOrHide(_getAnnotationTypeDoc().interfaces());
    }

    public boolean subclassOf(ClassDoc classdoc) {
        if (classdoc instanceof HidingAnnotationTypeDocWrapper) {
            classdoc = (AnnotationTypeDoc)
                       ((HidingAnnotationTypeDocWrapper)classdoc)
                         .getWrappedObject();
        }
        
        return _getAnnotationTypeDoc().subclassOf(classdoc);
    }

    public Type superclassType() {
        return (Type)wrapOrHide(_getAnnotationTypeDoc().superclassType());
    }

    public ClassDoc superclass() {
        return (ClassDoc)wrapOrHide(_getAnnotationTypeDoc().superclass());
    }

    public boolean definesSerializableFields() {
        return _getAnnotationTypeDoc().definesSerializableFields();
    }

    public FieldDoc[] serializableFields() {
        return (FieldDoc[])
                wrapOrHide(_getAnnotationTypeDoc().serializableFields());
    }

    public MethodDoc[] serializationMethods() {
        return (MethodDoc[])
                wrapOrHide(_getAnnotationTypeDoc().serializationMethods());
    }

    public boolean isExternalizable() {
        return _getAnnotationTypeDoc().isExternalizable();
    }

    public boolean isSerializable() {
        return _getAnnotationTypeDoc().isSerializable();
    }

    public boolean isAbstract() {
        return _getAnnotationTypeDoc().isAbstract();
    }

    public boolean isFinal() {
        return _getAnnotationTypeDoc().isFinal();
    }

    public boolean isStatic() {
        return _getAnnotationTypeDoc().isStatic();
    }

    public boolean isPackagePrivate() {
        return _getAnnotationTypeDoc().isPackagePrivate();
    }

    public boolean isPrivate() {
        return _getAnnotationTypeDoc().isPrivate();
    }

    public boolean isProtected() {
        return _getAnnotationTypeDoc().isProtected();
    }

    public boolean isPublic() {
        return _getAnnotationTypeDoc().isPublic();
    }

    public AnnotationDesc[] annotations() {
        return (AnnotationDesc[])
                wrapOrHide(_getAnnotationTypeDoc().annotations());
    }

    public String modifiers() {
        return _getAnnotationTypeDoc().modifiers();
    }

    public int modifierSpecifier() {
        return _getAnnotationTypeDoc().modifierSpecifier();
    }

    public String qualifiedName()  {
        return _getAnnotationTypeDoc().qualifiedName();
    }

    public PackageDoc containingPackage() {
        return (PackageDoc)
                wrapOrHide(_getAnnotationTypeDoc().containingPackage());
    }

    public ClassDoc containingClass() {
        return (ClassDoc)wrapOrHide(_getAnnotationTypeDoc().containingClass());
    }

    public SourcePosition position() {
        return (SourcePosition)wrapOrHide(_getAnnotationTypeDoc().position());
    }

    public boolean isIncluded() {
        return _getAnnotationTypeDoc().isIncluded();
    }

    public boolean isClass() {
        return _getAnnotationTypeDoc().isClass();
    }

    public boolean isOrdinaryClass() {
        return _getAnnotationTypeDoc().isOrdinaryClass();
    }

    public boolean isAnnotationType() {
        return _getAnnotationTypeDoc().isAnnotationType();
    }

    public boolean isEnum() {
        return _getAnnotationTypeDoc().isEnum();
    }

    public boolean isError() {
        return _getAnnotationTypeDoc().isError();
    }

    public boolean isException() {
        return _getAnnotationTypeDoc().isException();
    }

    public boolean isInterface() {
        return _getAnnotationTypeDoc().isInterface();
    }

    public boolean isAnnotationTypeElement() {
        return _getAnnotationTypeDoc().isAnnotationTypeElement();
    }

    public boolean isMethod() {
        return _getAnnotationTypeDoc().isMethod();
    }

    public boolean isConstructor() {
        return _getAnnotationTypeDoc().isConstructor();
    }

    public boolean isEnumConstant() {
        return _getAnnotationTypeDoc().isEnumConstant();
    }

    public boolean isField() {
        return _getAnnotationTypeDoc().isField();
    }

    public int compareTo(Object obj) {
        if (1==1) {
            return 0;
        }

        if (obj instanceof HidingWrapper) {
            return compareTo(((HidingWrapper)obj).getWrappedObject());
        } else {
            return _getAnnotationTypeDoc().compareTo(obj);
        }
    }

    public String name() {
        return _getAnnotationTypeDoc().name();
    }

    public void setRawCommentText(String text) {
        _getAnnotationTypeDoc().setRawCommentText(text);
    }

    public String getRawCommentText() {
        return _getAnnotationTypeDoc().getRawCommentText();
    }

    public Tag[] firstSentenceTags() {
        return (Tag[])wrapOrHide(_getAnnotationTypeDoc().firstSentenceTags());
    }

    public Tag[] inlineTags() {
        return (Tag[])wrapOrHide(_getAnnotationTypeDoc().inlineTags());
    }

    public SeeTag[] seeTags() {
        return (SeeTag[])wrapOrHide(_getAnnotationTypeDoc().seeTags());
    }

    public Tag[] tags() {
        return (Tag[])wrapOrHide(_getAnnotationTypeDoc().tags());
    }

    public Tag[] tags(String szTagName) {
        return (Tag[])wrapOrHide(_getAnnotationTypeDoc().tags(szTagName));
    }

    public String commentText() {
        return _getAnnotationTypeDoc().commentText();
    }

    public AnnotationTypeElementDoc[] elements() {
        return (AnnotationTypeElementDoc[])
                wrapOrHide(_getAnnotationTypeDoc().elements());
    }
    
    public ClassDoc asClassDoc() {
        return (ClassDoc)wrapOrHide(_getAnnotationTypeDoc().asClassDoc());
    }

    public String dimension() {
        return _getAnnotationTypeDoc().dimension();
    }

    public String qualifiedTypeName() {
        return _getAnnotationTypeDoc().qualifiedTypeName();
    }

    public String toString() {
        return _getAnnotationTypeDoc().toString();
    }

    public String typeName() {
        return _getAnnotationTypeDoc().typeName();
    }

    public AnnotationTypeDoc asAnnotationTypeDoc() {
        return (AnnotationTypeDoc)
                wrapOrHide(_getAnnotationTypeDoc().asAnnotationTypeDoc());
    }

    public WildcardType asWildcardType() {
        return (WildcardType)
                wrapOrHide(_getAnnotationTypeDoc().asWildcardType());
    }

    public TypeVariable asTypeVariable() {
        return (TypeVariable)
                wrapOrHide(_getAnnotationTypeDoc().asTypeVariable());
    }

    public ParameterizedType asParameterizedType() {
        return (ParameterizedType)wrapOrHide(_getAnnotationTypeDoc()
                                             .asParameterizedType());
    }

    public boolean isPrimitive() {
        return _getAnnotationTypeDoc().isPrimitive();
    }
 
    public String simpleTypeName() {
        return _getAnnotationTypeDoc().simpleTypeName();
    }
}
